use std::ffi::CStr;
use std::mem::MaybeUninit;
use std::pin::pin;
use std::time::Duration;

use pgrx::bgworkers::{
    BackgroundWorker, BackgroundWorkerBuilder, BgWorkerStartTime, SignalWakeFlags,
};
use pgrx::prelude::*;
use picomq_runtime::{PicoServer, RuntimeError, ServerConfig};
use tokio::runtime::Runtime;
use tracing_subscriber::EnvFilter;

use crate::config::{self, Local};
use crate::guc::Settings;

const RESTART: Duration = Duration::from_secs(5);
const SHUTDOWN: Duration = Duration::from_secs(2);
const TICK: Duration = Duration::from_secs(1);

pub(crate) fn register() {
    BackgroundWorkerBuilder::new("pico")
        .set_type("pico")
        .set_function("pico_worker_main")
        .set_library("pico")
        .enable_shmem_access(None)
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(RESTART))
        .load();
}

#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn pico_worker_main(_argument: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    let settings = Settings::load();
    let config = match config::server(&settings, &local()) {
        Ok(config) => config,
        Err(message) => {
            warning!("pico: not starting: {message}");
            return;
        }
    };
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(&settings.log))
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .try_init();
    let runtime = match runtime(settings.threads) {
        Ok(runtime) => runtime,
        Err(e) => error!("pico: runtime: {e}"),
    };
    let server = match start(&runtime, config) {
        Ok(Some(server)) => server,
        Ok(None) => {
            runtime.shutdown_background();
            return;
        }
        Err(e) => error!("pico: {e}"),
    };
    log!("pico: serving on {}", server.local_addr());
    while BackgroundWorker::wait_latch(Some(TICK)) {}
    if runtime
        .block_on(async { tokio::time::timeout(SHUTDOWN, server.shutdown()).await })
        .is_err()
    {
        warning!("pico: shutdown did not finish within {SHUTDOWN:?}");
    }
    runtime.shutdown_background();
}

fn start(runtime: &Runtime, config: ServerConfig) -> Result<Option<PicoServer>, RuntimeError> {
    runtime.block_on(async {
        let mut start = pin!(picomq_runtime::start(config));
        loop {
            tokio::select! {
                result = &mut start => return result.map(Some),
                () = tokio::time::sleep(TICK) => {
                    if BackgroundWorker::sigterm_received() {
                        return Ok(None);
                    }
                }
            }
        }
    })
}

fn runtime(threads: usize) -> std::io::Result<Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .thread_name("pico")
        .on_thread_start(deaf)
        .enable_all()
        .build()
}

fn deaf() {
    let mut set = MaybeUninit::<libc::sigset_t>::uninit();
    unsafe {
        libc::sigfillset(set.as_mut_ptr());
        for synchronous in [libc::SIGSEGV, libc::SIGBUS, libc::SIGFPE, libc::SIGILL] {
            libc::sigdelset(set.as_mut_ptr(), synchronous);
        }
        libc::pthread_sigmask(libc::SIG_BLOCK, set.as_ptr(), std::ptr::null_mut());
    }
}

fn local() -> Local {
    let directories = unsafe { pg_sys::Unix_socket_directories };
    let socket = (!directories.is_null())
        .then(|| unsafe { CStr::from_ptr(directories) }.to_string_lossy())
        .and_then(|directories| {
            directories
                .split(',')
                .map(str::trim)
                .find(|directory| !directory.is_empty())
                .filter(|directory| !directory.starts_with('@'))
                .map(str::to_owned)
        });
    let port = u16::try_from(unsafe { pg_sys::PostPortNumber }).unwrap_or(5432);
    let user = unsafe {
        let entry = libc::getpwuid(libc::geteuid());
        if entry.is_null() {
            "postgres".to_owned()
        } else {
            CStr::from_ptr((*entry).pw_name)
                .to_string_lossy()
                .into_owned()
        }
    };
    Local { socket, port, user }
}
