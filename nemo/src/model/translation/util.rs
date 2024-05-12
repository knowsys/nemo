pub fn number_to_letters(mut number: usize) -> String {
    let mut result = String::new();

    loop {
        let remainder = number % 26;
        result.insert(0, (b'a' + remainder as u8) as char);

        if let Some(next) = (number / 26).checked_sub(1) {
            number = next
        } else {
            break;
        }
    }

    result
}
