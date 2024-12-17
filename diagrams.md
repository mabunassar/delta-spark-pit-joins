I generated the base features table with the `mock_data` specified in `FeaturesTable` class in utils.py, the table looks like below:
```
+----------+-----------+-------------------+----------------------------------+----------------------------------+---------------------------------+---------------------------------+
|account_id| contact_id|          timestamp|f_contact_click_count_over_30_days|f_contact_click_count_over_60_days|f_contact_open_count_over_30_days|f_contact_open_count_over_60_days|
+----------+-----------+-------------------+----------------------------------+----------------------------------+---------------------------------+---------------------------------+
|       100|second_user|2024-12-03 08:00:00|                                 0|                                 5|                               10|                               15|
|       101| first_user|2024-12-07 10:00:00|                                20|                                40|                               20|                               40|
|       100| first_user|2024-12-01 06:00:00|                                20|                                40|                               20|                               40|
|       102| third_user|2024-12-05 07:00:00|                               100|                               100|                              100|                              100|
+----------+-----------+-------------------+----------------------------------+----------------------------------+---------------------------------+---------------------------------+
```

I then performed a "Merge"/"Upsert" where we "update all cols" unless all columns (except timestamp) of the new potential entry matches with an entry in the existing table, then we only update the timestamp column.

Here's the table after updating the table with the `batch_data` specified in `FeaturesTable` class in utils.py
```
+----------+-----------+-------------------+----------------------------------+----------------------------------+---------------------------------+---------------------------------+
|account_id| contact_id|          timestamp|f_contact_click_count_over_30_days|f_contact_click_count_over_60_days|f_contact_open_count_over_30_days|f_contact_open_count_over_60_days|
+----------+-----------+-------------------+----------------------------------+----------------------------------+---------------------------------+---------------------------------+
|       100|second_user|2024-12-03 08:00:00|                                 0|                                 5|                               10|                               15|
|       101| first_user|2024-12-09 10:00:00|                                20|                                40|                               20|                               40| #Timestamp got updated as expected
|       100| first_user|2024-12-01 06:00:00|                                20|                                40|                               20|                               40|
|       102| third_user|2024-12-05 07:00:00|                               100|                               100|                              100|                              100|
|       100| first_user|2024-12-02 06:00:00|                                40|                                80|                               40|                               80|
|       100|fourth_user|2024-12-11 09:00:00|                               200|                               400|                              200|                              400|
+----------+-----------+-------------------+----------------------------------+----------------------------------+---------------------------------+---------------------------------+
```

Now it's time to do the point in time joins (PIT Joins) where our left table is the observations tables and for each observation we bring the most recent feature computed:

```
+----------+----------+-------------------+----------+----------+----------+-------------------+----------------------------------+----------------------------------+---------------------------------+---------------------------------+
|account_id|contact_id|          timestamp|event_type|account_id|contact_id|          timestamp|f_contact_click_count_over_30_days|f_contact_click_count_over_60_days|f_contact_open_count_over_30_days|f_contact_open_count_over_60_days|
+----------+----------+-------------------+----------+----------+----------+-------------------+----------------------------------+----------------------------------+---------------------------------+---------------------------------+
|       100|first_user|2024-12-01 06:01:00|     click|       100|first_user|2024-12-01 06:00:00|                                20|                                40|                               20|                               40|
|       100|first_user|2024-12-02 12:00:00|     click|       100|first_user|2024-12-02 06:00:00|                                40|                                80|                               40|                               80|
|       101|first_user|2024-12-07 11:00:00|      open|      NULL|      NULL|               NULL|                              NULL|                              NULL|                             NULL|                             NULL|
|       102|third_user|2024-12-04 00:00:00|      open|      NULL|      NULL|               NULL|                              NULL|                              NULL|                             NULL|                             NULL|
+----------+----------+-------------------+----------+----------+----------+-------------------+----------------------------------+----------------------------------+---------------------------------+---------------------------------+
```