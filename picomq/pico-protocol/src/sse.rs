pub(crate) fn lines(text: &str) -> impl Iterator<Item = &str> {
    text.split("\r\n")
        .flat_map(|chunk| chunk.split(['\r', '\n']))
}
