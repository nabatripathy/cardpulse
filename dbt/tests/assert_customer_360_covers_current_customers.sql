-- Customer 360 has exactly the current customers.
select coalesce(d.customer_id, c.customer_id) as customer_id,
       d.customer_id is null as missing_from_dimension,
       c.customer_id is null as missing_from_customer_360
from (select customer_id from {{ ref('dim_customer') }} where is_current) d
full join {{ ref('customer_360') }} c using (customer_id)
where d.customer_id is null or c.customer_id is null
