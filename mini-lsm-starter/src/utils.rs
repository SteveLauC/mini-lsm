pub fn display_utf8_bytes(bytes: &[u8]) -> impl std::fmt::Display {
    let str = std::str::from_utf8(bytes).unwrap();
    str.to_string()
}