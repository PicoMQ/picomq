//! Credentials and the `pico auth` command group.
//!
//! Tokens live in the OS keyring under the profile name, with a private
//! credentials file next to the config as the fallback. A token found in the
//! file while the keyring works is migrated into the keyring. Explicit
//! `--token` and `PICO_TOKEN` always win over storage.

use std::collections::BTreeMap;
use std::io::Read;
use std::path::PathBuf;

use clap::{Args, Subcommand};
use pico_auth::AccessToken;

use crate::io::note;
use crate::stream::Endpoint;

const KEYRING_SERVICE: &str = "picomq";

/// The profile a credential is filed under: the explicit `--profile`, else the
/// configured default, else a fixed name so `pico auth login` works before any
/// profile exists.
pub fn profile_name(explicit: Option<&str>) -> Result<String, String> {
    if let Some(name) = explicit {
        return Ok(name.to_owned());
    }
    Ok(crate::config::load()?
        .default_profile
        .unwrap_or_else(|| "default".to_owned()))
}

/// The stored token for a profile, keyring first, then the fallback file.
pub fn lookup(profile: &str) -> Result<Option<String>, String> {
    if let Some(entry) = keyring_entry(profile) {
        match entry.get_password() {
            Ok(token) => return Ok(Some(token)),
            Err(keyring::Error::NoEntry) => {}
            // A locked or broken keyring degrades to the file, it does not
            // block every command.
            Err(_) => return file_get(profile),
        }
        let Some(token) = file_get(profile)? else {
            return Ok(None);
        };
        if entry.set_password(&token).is_ok() {
            file_remove(profile)?;
        }
        return Ok(Some(token));
    }
    file_get(profile)
}

/// Returns where the token landed.
pub fn store(profile: &str, token: &str) -> Result<&'static str, String> {
    if let Some(entry) = keyring_entry(profile) {
        if entry.set_password(token).is_ok() {
            // The file copy is now stale at best.
            file_remove(profile)?;
            return Ok("keyring");
        }
    }
    file_set(profile, token)?;
    Ok("credentials file")
}

pub fn remove(profile: &str) -> Result<bool, String> {
    let mut removed = false;
    if let Some(entry) = keyring_entry(profile) {
        removed |= entry.delete_credential().is_ok();
    }
    removed |= file_remove(profile)?;
    Ok(removed)
}

/// `PICO_NO_KEYRING` skips the OS keyring entirely (headless machines, CI).
fn keyring_entry(profile: &str) -> Option<keyring::Entry> {
    if std::env::var_os("PICO_NO_KEYRING").is_some() {
        return None;
    }
    keyring::Entry::new(KEYRING_SERVICE, profile).ok()
}

/// `credentials.toml` next to the config file: a profile-to-token map,
/// written 0600.
fn file_path() -> Result<PathBuf, String> {
    let config = crate::config::path()?;
    let parent = config
        .parent()
        .ok_or_else(|| format!("{} has no parent directory", config.display()))?;
    Ok(parent.join("credentials.toml"))
}

fn file_load() -> Result<BTreeMap<String, String>, String> {
    let path = file_path()?;
    let text = match std::fs::read_to_string(&path) {
        Ok(text) => text,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(e) => return Err(format!("{}: {e}", path.display())),
    };
    toml::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))
}

fn file_save(entries: &BTreeMap<String, String>) -> Result<(), String> {
    let path = file_path()?;
    if entries.is_empty() {
        match std::fs::remove_file(&path) {
            Ok(()) => return Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(format!("{}: {e}", path.display())),
        }
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("{}: {e}", parent.display()))?;
    }
    let text = toml::to_string_pretty(entries).map_err(|e| e.to_string())?;
    std::fs::write(&path, text).map_err(|e| format!("{}: {e}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .map_err(|e| format!("{}: {e}", path.display()))?;
    }
    Ok(())
}

fn file_get(profile: &str) -> Result<Option<String>, String> {
    Ok(file_load()?.get(profile).cloned())
}

fn file_set(profile: &str, token: &str) -> Result<(), String> {
    let mut entries = file_load()?;
    entries.insert(profile.to_owned(), token.to_owned());
    file_save(&entries)
}

fn file_remove(profile: &str) -> Result<bool, String> {
    let mut entries = file_load()?;
    let removed = entries.remove(profile).is_some();
    if removed {
        file_save(&entries)?;
    }
    Ok(removed)
}

#[derive(Debug, Subcommand)]
pub enum AuthCommand {
    /// Store a token for the selected profile.
    Login(LoginArgs),
    /// Remove the stored token for the selected profile.
    Logout,
    /// Where the credential lives, its id, and whether the endpoint takes it.
    Status,
}

#[derive(Debug, Args)]
pub struct LoginArgs {
    /// The token in wire form. Read from stdin when absent, so the token
    /// stays out of shell history.
    #[arg(long)]
    pub token: Option<String>,
}

pub async fn run(command: AuthCommand, flags: &Endpoint) -> Result<i32, String> {
    let profile = profile_name(flags.profile.as_deref())?;
    match command {
        AuthCommand::Login(args) => {
            let token = match args.token {
                Some(token) => token,
                None => {
                    note("paste the token, then enter (or ctrl-d)");
                    let mut raw = String::new();
                    std::io::stdin()
                        .read_to_string(&mut raw)
                        .map_err(|e| e.to_string())?;
                    raw.trim().to_owned()
                }
            };
            let parsed =
                AccessToken::parse(&token).map_err(|_| "that is not a token".to_owned())?;
            let location = store(&profile, &token)?;
            note(format!(
                "stored token `{}` for profile `{profile}` in the {location}",
                parsed.id
            ));
        }
        AuthCommand::Logout => {
            if remove(&profile)? {
                note(format!("removed the token for profile `{profile}`"));
            } else {
                note(format!("no token stored for profile `{profile}`"));
            }
        }
        AuthCommand::Status => {
            let Some(token) = lookup(&profile)? else {
                println!("profile={profile} token=none");
                return Ok(1);
            };
            let id = AccessToken::parse(&token)
                .map(|t| t.id)
                .unwrap_or_else(|_| "(unparseable)".to_owned());
            print!("profile={profile} token={id}");
            let endpoint = crate::config::selected(flags.profile.as_deref())?
                .endpoint
                .or_else(|| flags.endpoint.clone());
            let Some(endpoint) = endpoint else {
                println!(" endpoint=none");
                return Ok(0);
            };
            // Any answer but 401 means the server accepted the credential.
            // 403 is authenticated but out of scope for a root list.
            let response = reqwest::Client::new()
                .get(format!("{}/", endpoint.trim_end_matches('/')))
                .bearer_auth(&token)
                .send()
                .await
                .map_err(|e| e.to_string())?;
            let verdict = match response.status().as_u16() {
                401 => "rejected",
                403 => "authenticated (out of scope for listing)",
                _ => "authenticated",
            };
            println!(" endpoint={endpoint} status={verdict}");
            if response.status() == 401 {
                return Ok(1);
            }
        }
    }
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One test body: these mutate process env (`PICO_CONFIG`), which is not
    /// safe across parallel tests.
    #[test]
    fn file_fallback_round_trip_private_and_removable() {
        let dir = tempfile::tempdir().unwrap();
        std::env::set_var("PICO_CONFIG", dir.path().join("config.toml"));
        std::env::set_var("PICO_NO_KEYRING", "1");

        assert_eq!(profile_name(Some("prod")).unwrap(), "prod");
        assert_eq!(profile_name(None).unwrap(), "default");

        assert_eq!(lookup("default").unwrap(), None);
        assert_eq!(store("default", "tok-a").unwrap(), "credentials file");
        assert_eq!(store("other", "tok-b").unwrap(), "credentials file");
        assert_eq!(lookup("default").unwrap().as_deref(), Some("tok-a"));
        assert_eq!(lookup("other").unwrap().as_deref(), Some("tok-b"));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(dir.path().join("credentials.toml"))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600, "credentials stay private");
        }

        assert!(remove("default").unwrap());
        assert!(!remove("default").unwrap());
        assert_eq!(lookup("default").unwrap(), None);
        assert_eq!(lookup("other").unwrap().as_deref(), Some("tok-b"));

        // Removing the last entry removes the file itself.
        assert!(remove("other").unwrap());
        assert!(!dir.path().join("credentials.toml").exists());
    }
}
