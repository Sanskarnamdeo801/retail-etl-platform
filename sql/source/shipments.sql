CREATE TABLE IF NOT EXISTS shipments (
    shipment_id INT PRIMARY KEY,
    order_id INT,
    shipment_date TIMESTAMP,
    delivery_date TIMESTAMP,
    carrier VARCHAR(100),
    shipment_status VARCHAR(50),
    tracking_number VARCHAR(100),
    CONSTRAINT fk_shipments_order
        FOREIGN KEY (order_id) REFERENCES orders(order_id)
);