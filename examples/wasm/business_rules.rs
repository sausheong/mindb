// business_rules.rs - Business logic stored procedures
// Compile: rustc --target wasm32-unknown-unknown -O business_rules.rs

#[no_mangle]
pub extern "C" fn calculate_tax(amount: f64, state_code: i32) -> f64 {
    let tax_rate = match state_code {
        1 => 0.05,   // California: 5%
        2 => 0.07,   // New York: 7%
        3 => 0.10,   // Texas: 10%
        4 => 0.06,   // Florida: 6%
        5 => 0.08,   // Illinois: 8%
        _ => 0.08,   // Default: 8%
    };
    
    amount * tax_rate
}

#[no_mangle]
pub extern "C" fn calculate_shipping(weight: f64, distance: f64, express: i32) -> f64 {
    let base_rate = 5.0;
    let weight_rate = 0.5;
    let distance_rate = 0.1;
    
    let standard_cost = base_rate + (weight * weight_rate) + (distance * distance_rate);
    
    if express != 0 {
        standard_cost * 1.5  // Express shipping is 50% more
    } else {
        standard_cost
    }
}

#[no_mangle]
pub extern "C" fn validate_credit_card(number: i64) -> i32 {
    // Luhn algorithm for credit card validation
    let mut sum = 0;
    let mut num = number;
    let mut alternate = false;
    
    while num > 0 {
        let mut digit = (num % 10) as i32;
        num /= 10;
        
        if alternate {
            digit *= 2;
            if digit > 9 {
                digit -= 9;
            }
        }
        
        sum += digit;
        alternate = !alternate;
    }
    
    if sum % 10 == 0 { 1 } else { 0 }
}

#[no_mangle]
pub extern "C" fn calculate_commission(sales: f64, tier: i32) -> f64 {
    let commission_rate = match tier {
        1 => 0.05,   // Junior: 5%
        2 => 0.08,   // Mid: 8%
        3 => 0.12,   // Senior: 12%
        4 => 0.15,   // Manager: 15%
        _ => 0.03,   // Default: 3%
    };
    
    sales * commission_rate
}

#[no_mangle]
pub extern "C" fn calculate_late_fee(days_late: i32, amount: f64) -> f64 {
    if days_late <= 0 {
        return 0.0;
    }
    
    let daily_rate = 0.01;  // 1% per day
    let max_rate = 0.25;    // Cap at 25%
    
    let fee_rate = (days_late as f64 * daily_rate).min(max_rate);
    amount * fee_rate
}
