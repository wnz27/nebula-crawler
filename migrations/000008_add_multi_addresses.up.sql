CREATE TABLE IF NOT EXISTS multi_addresses
(
    -- A unique id that identifies this multi address
    id         SERIAL,
    -- The peer ID in the form of Qm... or 12D3...
    peer_id    VARCHAR(100) NOT NULL,
    -- The multi address
    address    VARCHAR(100) NOT NULL,
    -- When was this multi_address updated the last time
    updated_at TIMESTAMPTZ  NOT NULL,
    -- When was this multi_address instance created
    created_at TIMESTAMPTZ  NOT NULL,

    PRIMARY KEY (id)
);
