#![feature(trait_alias)]

pub mod task;
pub mod worker;
pub mod processors;
pub mod messages;
pub mod types;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = "We don't need tests where we are swimming to the top";
        assert_eq!(result, "We don't need tests where we are swimming to the top");
    }
}
