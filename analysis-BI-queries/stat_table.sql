with btc_stat as
(
  select 
      gotten_at::date as gotten_at, 
	  round(avg(price), 2) as price
    from crpccy.okx_rates
	where inst_id = 'BTC-USD'
	group by gotten_at::date
),
general_stat as
(
  select 
      c.gotten_at as dt,
	  c.price,
	  i.rate_value as rate
	from btc_stat c
      join crpccy.fng_rates i
	    on c.gotten_at = i.gotten_at
)
select 
    s1.dt, 
	s1.price, 
	round((s1.price - s2.price) * 100.0 / s2.price, 2) || '%' as price_delta,
	s1.rate as fear_date,
	round((s1.rate - s2.rate) * 100.0 / s2.rate, 2) || '%' as fear_rate_delta
  from general_stat s1
    left join general_stat s2
      on s1.dt = s2.dt + 1
  order by s1.dt desc