#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Config {
    pub catalog_size: i64,
    pub initial_stock: i64,
    pub threshold: i64,
    pub window_size: i64,
    pub slide_interval: i64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            catalog_size: 2000,
            initial_stock: 10000,
            threshold: 50,
            window_size: 1000,
            slide_interval: 500,
        }
    }
}
