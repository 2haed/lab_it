unique_planes = 'select distinct type from aircraft'
earliest_certificate = 'select type from aircraft where date = (select min(date) from aircraft)'
