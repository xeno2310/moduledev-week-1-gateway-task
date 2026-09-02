SET ROLE course_owner;

ALTER TABLE payment.operations
    ADD CONSTRAINT payment_operations_amount_range_check
    CHECK (
        amount >= 0.01
        AND amount <= 9999999999999999.99
    );

RESET ROLE;