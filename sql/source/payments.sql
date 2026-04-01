CREATE TABLE IF NOT EXISTS payments (
    payment_id INT PRIMARY KEY,
    order_id INT,
    payment_date TIMESTAMP,
    payment_method VARCHAR(50),
    payment_status VARCHAR(50),
    amount NUMERIC(10,2),
    CONSTRAINT fk_payments_order
        FOREIGN KEY (order_id) REFERENCES orders(order_id)
);