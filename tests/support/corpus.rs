// SPDX-License-Identifier: Apache-2.0

//! Reproducible HTML covering constructs sensitive to upload boundaries.

pub fn hazard_document() -> Vec<u8> {
    let mut document = String::from(
        "<!doctype html><html><head><meta charset=utf-8><title>Split &amp; join</title></head><body>",
    );
    let prose = "α😀 &amp; &#x1F600; boundary text. ".repeat(96);
    while document.len() < 2 * 1024 * 1024 {
        document.push_str("<section bare empty=\"\" title=\"雪😀 &amp; value\"><p>");
        document.push_str(&prose);
        document.push_str("</p><!--split opener--><svg><![CDATA[雪]]></svg><script>let x='&amp; </scr' + 'ipt>';</script></section>");
    }
    document.push_str("</body></html>");
    document.into_bytes()
}

pub fn next_random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}
