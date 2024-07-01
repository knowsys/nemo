//! LSP position:
//!
//! * line: u32 index of the line, first line gets index 0
//! * offset: u32 index of the UTF-16 code point within the line, first column gets index 0
//!
//! Nemo position:
//!
//! * line: u32 index of the line, first line gets index 1
//! * offset: u32 index of the UTF-8 code point (byte) within the line, first column gets index 0

use anyhow::anyhow;
use line_index::{LineCol, LineIndex, WideEncoding, WideLineCol};

#[derive(Debug)]
pub enum PositionConversionError {
    NemoPosition(nemo::io::parser::ast::Position),
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
) -> Result<nemo::io::parser::ast::Position, PositionConversionError> {
    Ok(nemo::io::parser::ast::Position {
        line: line_col.line + 1,
        column: line_col.col,
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
) -> Result<nemo::io::parser::ast::Position, PositionConversionError> {
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

fn nemo_position_to_line_col(position: nemo::io::parser::ast::Position) -> LineCol {
    LineCol {
        line: position.line - 1,
        col: position.column - 1,
    }
}

/// Converts a source position to a LSP position
pub fn nemo_position_to_lsp_position(
    line_index: &LineIndex,
    position: nemo::io::parser::ast::Position,
) -> Result<tower_lsp::lsp_types::Position, PositionConversionError> {
    // TODO: Find out what UTF encoding nemo parser uses
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
    range: nemo::io::parser::ast::Range,
) -> Result<tower_lsp::lsp_types::Range, PositionConversionError> {
    Ok(tower_lsp::lsp_types::Range {
        start: nemo_position_to_lsp_position(line_index, range.start)?,
        end: nemo_position_to_lsp_position(line_index, range.end)?,
    })
}
