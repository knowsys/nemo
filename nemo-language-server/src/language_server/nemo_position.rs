//! LSP position:
//!
//! * line: u32 index of the line, first line gets index 0
//! * character: u32 index of the UTF-16 code point within the line, first column gets index 0
//!
//! Nemo position:
//!
//! * line: u32 index of the line, first line gets index 1
//! * column: u32 index of the UTF-8 character within the line, first column gets index 1
//! * offset: usize index of the UTF-8 code point (byte) from the start of the parser input (0-indexed)

use anyhow::anyhow;
use line_index::{LineCol, LineIndex, WideEncoding, WideLineCol};
use nemo::parser::span::{CharacterPosition, CharacterRange};

#[derive(Debug)]
pub enum PositionConversionError {
    NemoPosition(CharacterPosition),
    LspPosition(tower_lsp::lsp_types::Position),
    LspLineCol(LineCol),
}

impl From<PositionConversionError> for anyhow::Error {
    fn from(val: PositionConversionError) -> Self {
        anyhow!("could not convert source code position: {:#?}", val)
    }
}

fn line_col_to_nemo_position(
    line_index: &LineIndex,
    line_col: LineCol,
) -> Result<CharacterPosition, PositionConversionError> {
    Ok(CharacterPosition {
        line: line_col.line + 1,
        column: line_col.col + 1,
        offset: line_index
            .offset(line_col)
            .ok_or(PositionConversionError::LspLineCol(line_col))?
            .into(),
    })
}

/// Converts a LSP position to a Nemo parser position
pub fn lsp_position_to_nemo_position(
    line_index: &LineIndex,
    position: tower_lsp::lsp_types::Position,
) -> Result<CharacterPosition, PositionConversionError> {
    let line_col = line_index
        .to_utf8(
            WideEncoding::Utf16,
            WideLineCol {
                line: position.line,
                col: position.character,
            },
        )
        .ok_or(PositionConversionError::LspPosition(position))?;

    line_col_to_nemo_position(line_index, line_col)
}

fn nemo_position_to_line_col(position: CharacterPosition) -> LineCol {
    LineCol {
        line: position.line - 1,
        col: position.column - 1,
    }
}

/// Converts a source position to a LSP position
pub fn nemo_position_to_lsp_position(
    line_index: &LineIndex,
    position: CharacterPosition,
) -> Result<tower_lsp::lsp_types::Position, PositionConversionError> {
    let wide_line_col = line_index
        .to_wide(WideEncoding::Utf16, nemo_position_to_line_col(position))
        .ok_or(PositionConversionError::NemoPosition(position))?;

    Ok(tower_lsp::lsp_types::Position {
        line: wide_line_col.line,
        character: wide_line_col.col,
    })
}

/// Converts a Nemo range to a LSP range
pub fn nemo_range_to_lsp_range(
    line_index: &LineIndex,
    range: CharacterRange,
) -> Result<tower_lsp::lsp_types::Range, PositionConversionError> {
    Ok(tower_lsp::lsp_types::Range {
        start: nemo_position_to_lsp_position(line_index, range.start)?,
        end: nemo_position_to_lsp_position(line_index, range.end)?,
    })
}
