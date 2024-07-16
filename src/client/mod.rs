//! Summerset's client functionality modules and trait.

mod endpoint;

mod apistub;
mod ctrlstub;

pub use ctrlstub::ClientCtrlStub;
pub use endpoint::{ClientId, GenericEndpoint};

pub(crate) use apistub::ClientApiStub;
