//! Functionality to read and write data is implemented here.
//!
//! This module acts as a mediation layer between the logical and physical layer and offers traits to allow both layers an abstract view on the io process.

pub mod formats;
pub mod input_manager;
pub mod output_manager;
pub mod parser;
pub mod resource_providers;

pub use input_manager::InputManager;
pub use output_manager::OutputManager;
