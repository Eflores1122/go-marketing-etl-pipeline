SELECT COUNT(DISTINCT tu.tracked_user_id) AS multi_channel_converters
FROM tracked_user tu
         JOIN event e_conv ON e_conv.tracked_user_id = tu.tracked_user_id AND e_conv.event_type = 'conversion'
WHERE EXISTS (
    SELECT 1 FROM event e_g
    WHERE e_g.tracked_user_id = tu.tracked_user_id
      AND e_g.source = 'Google Ads'
      AND e_g.event_type = 'click'
      AND e_g.event_time < e_conv.event_time
)
  AND EXISTS (
    SELECT 1 FROM event e_f
    WHERE e_f.tracked_user_id = tu.tracked_user_id
      AND e_f.source = 'Facebook Ads'
      AND e_f.event_type = 'click'
      AND e_f.event_time < e_conv.event_time
);

/* Resultado:
multi_channel_converters
0
*/