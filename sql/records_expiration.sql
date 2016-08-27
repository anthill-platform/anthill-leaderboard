CREATE EVENT `records_expiration`
ON SCHEDULE EVERY 5 MINUTE STARTS CURRENT_TIMESTAMP
DO
   DELETE FROM `records`
    WHERE NOW() > `records`.expire_at;