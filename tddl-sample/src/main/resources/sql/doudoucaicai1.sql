select date_format(date_time,'%m-%d') label,sum(data_value)/100 value
         from adam_stream_sample_collect
         where index_id = 110004
and date_time >= ?
         and date_time < ?

         group by date(date_time)
