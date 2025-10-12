// discount.rs - Simple discount calculation stored procedure
// Compile: rustc --target wasm32-unknown-unknown --crate-type=cdylib -O discount.rs -o discount.wasm

#![crate_type = "cdylib"]

#[no_mangle]
pub extern "C" fn calculate_discount(price: f64, tier: i32) -> f64 {
    match tier {
        1 => price * 0.95,  // 5% discount for tier 1
        2 => price * 0.90,  // 10% discount for tier 2
        3 => price * 0.85,  // 15% discount for tier 3
        4 => price * 0.80,  // 20% discount for tier 4
        5 => price * 0.75,  // 25% discount for tier 5
        _ => price,         // No discount for other tiers
    }
}

#[no_mangle]
pub extern "C" fn calculate_bulk_discount(price: f64, quantity: i32) -> f64 {
    let discount_rate = if quantity >= 100 {
        0.20  // 20% off for 100+
    } else if quantity >= 50 {
        0.15  // 15% off for 50+
    } else if quantity >= 10 {
        0.10  // 10% off for 10+
    } else {
        0.0   // No discount
    };
    
    price * (1.0 - discount_rate)
}

#[no_mangle]
pub extern "C" fn calculate_loyalty_points(amount: f64, is_premium: i32) -> i32 {
    let base_points = (amount / 10.0) as i32;  // 1 point per $10
    
    if is_premium != 0 {
        base_points * 2  // Premium members get 2x points
    } else {
        base_points
    }
}
