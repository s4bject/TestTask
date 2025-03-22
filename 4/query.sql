SELECT phrase,
       groupArray((hour, views_diff)) AS views_by_hour
FROM (
         SELECT phrase,
                toHour(dt)              AS hour,
                max(views) - min(views) AS views_diff
         FROM phrases_views
         WHERE campaign_id = 1111111
           AND toDate(dt) = toDate('2025-01-01')
         GROUP BY phrase, hour
         HAVING views_diff > 0
         ORDER BY hour DESC
         )
GROUP BY phrase;
