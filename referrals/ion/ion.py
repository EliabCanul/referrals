import pandas as pd
from netadata.ion.ion import push_table



QUERY_REFERRALS = """
select 
duh.OrderId, 
duh.CreatedOnUtc as OrderUTC,  
d.CouponCode, 
c.Id as ClientId,
c.username as Referrer_num, # changed Referrer to Referrer_num
a.Firstname as Referrer_Name, 
dd.Firstname as Referral_Name,
dd.PhoneNumber as Referral_num, # added
b.Id as childId, #added
o.OrderTotal as GMV_after_discount,
date_format(date_sub(o.CreatedOnUtc, interval 6 hour), '%y%v') as week,
CASE
    WHEN date_format(b.CreatedOnUtc, '%y%j') < date_format(date_sub(o.CreatedOnUtc, interval 6 hour), '%y%j')THEN "OLD"
    ELSE "NEW CUSTOMER"
END as IsNewCustomer,
s.Name as StoreName,
s.Id as StoreId,
s.ZipCode

from discountusagehistory duh

inner join discount d
on d.Id = duh.DiscountID
inner join netamx.order o
on o.Id=duh.OrderId
inner join netamx.customer c
on CONVERT(c.ReferralCode USING utf8) = d.CouponCode
inner join address a
on a.Id = c.BillingAddress_Id
inner join netamx.customer b
on o.CustomerID = b.Id
inner join address dd
on dd.Id = b.BillingAddress_Id
inner join store s 
on o.StoreId = s.Id

where CouponCode like "%-%" and CouponCode not like "NETA%"

order by GMV_after_discount desc"""


QUERY_OLD = """
SELECT 
duh.OrderId, 
duh.CreatedOnUtc as OrderUTC,  
d.CouponCode,
c.Id as ClientId,
c.username as Referrer_num, # changed Referrer to Referrer_num
a.Firstname as Referrer_Name, 
a.Firstname as Referral_Name,
a.PhoneNumber as Referral_num, # added
o.OrderTotal as GMV_after_discount,
date_format(date_sub(o.CreatedOnUtc, interval 6 hour), '%y%v') as week,
CASE
    WHEN date_format(c.CreatedOnUtc, '%y%j') < date_format(date_sub(o.CreatedOnUtc, interval 6 hour), '%y%j')THEN "OLD"
    ELSE "NEW CUSTOMER"
END as IsNewCustomer,
s.Name as StoreName,
s.Id as StoreId,
s.ZipCode

FROM netamx.order o
left join discountusagehistory duh
on o.Id = duh.OrderId
left join discount d
on d.Id = duh.DiscountId
inner join customer c
on c.Id = o.CustomerId
#where d.Id = 3888;
# extras:
inner join address a
on a.Id = c.BillingAddress_Id
inner join store s 
on o.StoreId = s.Id

where CouponCode like "%-%" and CouponCode not like "NETA%"

order by CouponCode 

"""


QUERY_USERS_BEHAVIOR="""with datos as (
  select
    s.Id as storeid,
    cust.Id as cliente,
    p.Sku,
    oi.PriceExcltax,
    oi.Quantity * oi.OriginalProductCost as costo,
    c.Name as categoria,
    pcm.CategoryId as catid,
    s.ZipCode AS ZipCode_tienda,
    s.Latitud AS Latitude,
    s.Longitud AS Longitude,
    date(
      CASE
        WHEN TIME(date_sub(o.CreatedOnUtc, interval 6 HOUR)) <= '21:00:00' THEN DATE_ADD(
          date_sub(o.CreatedOnUtc, interval 6 HOUR),
          INTERVAL (
            CASE
              WHEN dayname(date_sub(o.CreatedOnUtc, interval 6 HOUR)) = 'Saturday' THEN 2
              ELSE 1
            END
          ) DAY
        )
        ELSE DATE_ADD(
          date_sub(o.CreatedOnUtc, interval 6 HOUR),
          INTERVAL (
            CASE
              WHEN dayname(date_sub(o.CreatedOnUtc, interval 6 HOUR)) = 'Friday' THEN 3
              ELSE 2
            END
          ) DAY
        )
      END
    ) AS fecha
  FROM
    netamx.Order o
    INNER JOIN netamx.OrderItem oi ON oi.OrderId = o.Id
    INNER JOIN netamx.Product p ON p.Id = oi.ProductId
    INNER JOIN netamx.Address AS a ON o.BillingAddressid = a.Id
    INNER JOIN netamx.Customer AS cust ON cust.Id = o.CustomerId
    INNER JOIN netamx.Store s ON o.StoreId = s.Id
    LEFT JOIN netamx.Product_Category_Mapping pcm ON p.Id = pcm.ProductId
    inner join category as c on c.Id = pcm.CategoryId
),
nacimientoclientes as (
  select
    min(
      STR_TO_DATE(CONCAT(YEARWEEK(fecha, 7), ' Monday'), '%x%v %W')
    ) as nacimiento,
    max(
      STR_TO_DATE(CONCAT(YEARWEEK(fecha, 7), ' Monday'), '%x%v %W')
    ) as ultimopedido,
    cliente as cliente
  from
    datos
  group by
    cliente
),
infoclientes as (
  select
    STR_TO_DATE(CONCAT(YEARWEEK(fecha, 7), ' Monday'), '%x%v %W') as semana,
    storeid as tienda,
    cliente as cliente,
    sum(PriceExclTax) as gmv,
    sum(costo) as costo,
    (sum(PriceExclTax) - sum(costo)) / sum(PriceExclTax) as gm,
    count(distinct sku) as skus,
    ZipCode_tienda as ZipCode_tienda,
    count(distinct categoria) as categorias
  from
    datos
  group by
    semana,
    tienda,
    cliente
),
frecuencia as (
  select
    fecha as fecha,
    cliente,
    case
      when sum(PriceExclTax) > 0 then 1
      else 0
    end as pedido
  from
    datos
  group by
    fecha,
    cliente
),
frecuenciasem as (
  select
    STR_TO_DATE(CONCAT(YEARWEEK(fecha, 7), ' Monday'), '%x%v %W') as semana,
    cliente,
    sum(pedido) as frecuencia
  from
    frecuencia
  group by
    semana,
    cliente
),
tiendas as (
  select
    STR_TO_DATE(CONCAT(YEARWEEK(fecha, 7), ' Monday'), '%x%v %W') as semana,
    storeid as tienda,
    Latitude as Latitude,
    Longitude as Longitude,
    count(distinct cliente) as clientesactivos
  from
    datos
  group by
    tienda,
    ZipCode_tienda,
    semana
),
historico as (
  select
    cliente,
    count(distinct sku) as skuhistoricos,
    count(distinct categoria) as catshistoricas
  from
    datos
  group by
    cliente
),
cat2 as (
  select
    STR_TO_DATE(CONCAT(YEARWEEK(fecha, 7), ' Monday'), '%x%v %W') as semana,
    cliente,
    case
      when sum(PriceExclTax) > 0 then 1
      else 0
    end as cuidadopersonal
  from
    datos
  where
    catid = 2
  group by
    semana,
    cliente
),
cat3 as (
  select
    STR_TO_DATE(CONCAT(YEARWEEK(fecha, 7), ' Monday'), '%x%v %W') as semana,
    cliente,
    case
      when sum(PriceExclTax) > 0 then 1
      else 0
    end as despensa
  from
    datos
  where
    catid = 3
  group by
    semana,
    cliente
),
cat7 as (
  select
    STR_TO_DATE(CONCAT(YEARWEEK(fecha, 7), ' Monday'), '%x%v %W') as semana,
    cliente,
    case
      when sum(PriceExclTax) > 0 then 1
      else 0
    end as fruver
  from
    datos
  where
    catid = 7
  group by
    semana,
    cliente
),
cat11 as (
  select
    STR_TO_DATE(CONCAT(YEARWEEK(fecha, 7), ' Monday'), '%x%v %W') as semana,
    cliente,
    case
      when sum(PriceExclTax) > 0 then 1
      else 0
    end as oficina
  from
    datos
  where
    catid = 11
  group by
    semana,
    cliente
),
cat12 as (
  select
    STR_TO_DATE(CONCAT(YEARWEEK(fecha, 7), ' Monday'), '%x%v %W') as semana,
    cliente,
    case
      when sum(PriceExclTax) > 0 then 1
      else 0
    end as tecnologia
  from
    datos
  where
    catid = 12
  group by
    semana,
    cliente
),
cat13 as (
  select
    STR_TO_DATE(CONCAT(YEARWEEK(fecha, 7), ' Monday'), '%x%v %W') as semana,
    cliente,
    case
      when sum(PriceExclTax) > 0 then 1
      else 0
    end as bazar
  from
    datos
  where
    catid = 13
  group by
    semana,
    cliente
),
cat14 as (
  select
    STR_TO_DATE(CONCAT(YEARWEEK(fecha, 7), ' Monday'), '%x%v %W') as semana,
    cliente,
    case
      when sum(PriceExclTax) > 0 then 1
      else 0
    end as mascotas
  from
    datos
  where
    catid = 14
  group by
    semana,
    cliente
),
cat15 as (
  select
    STR_TO_DATE(CONCAT(YEARWEEK(fecha, 7), ' Monday'), '%x%v %W') as semana,
    cliente,
    case
      when sum(PriceExclTax) > 0 then 1
      else 0
    end as bebidas
  from
    datos
  where
    catid = 15
  group by
    semana,
    cliente
),
cat68 as (
  select
    STR_TO_DATE(CONCAT(YEARWEEK(fecha, 7), ' Monday'), '%x%v %W') as semana,
    cliente,
    case
      when sum(PriceExclTax) > 0 then 1
      else 0
    end as licoresycig
  from
    datos
  where
    catid = 68
  group by
    semana,
    cliente
),
cat69 as (
  select
    STR_TO_DATE(CONCAT(YEARWEEK(fecha, 7), ' Monday'), '%x%v %W') as semana,
    cliente,
    case
      when sum(PriceExclTax) > 0 then 1
      else 0
    end as dulcesybotanas
  from
    datos
  where
    catid = 69
  group by
    semana,
    cliente
),
cat70 as (
  select
    STR_TO_DATE(CONCAT(YEARWEEK(fecha, 7), ' Monday'), '%x%v %W') as semana,
    cliente,
    case
      when sum(PriceExclTax) > 0 then 1
      else 0
    end as lacteosyrefri
  from
    datos
  where
    catid = 70
  group by
    semana,
    cliente
),
cat71 as (
  select
    STR_TO_DATE(CONCAT(YEARWEEK(fecha, 7), ' Monday'), '%x%v %W') as semana,
    cliente,
    case
      when sum(PriceExclTax) > 0 then 1
      else 0
    end as cuidadosalud
  from
    datos
  where
    catid = 71
  group by
    semana,
    cliente
),
cat72 as (
  select
    STR_TO_DATE(CONCAT(YEARWEEK(fecha, 7), ' Monday'), '%x%v %W') as semana,
    cliente,
    case
      when sum(PriceExclTax) > 0 then 1
      else 0
    end as cuidadodelhogar
  from
    datos
  where
    catid = 72
  group by
    semana,
    cliente
),
cat73 as (
  select
    STR_TO_DATE(CONCAT(YEARWEEK(fecha, 7), ' Monday'), '%x%v %W') as semana,
    cliente,
    case
      when sum(PriceExclTax) > 0 then 1
      else 0
    end as canastabasica
  from
    datos
  where
    catid = 73
  group by
    semana,
    cliente
),
cat74 as (
  select
    STR_TO_DATE(CONCAT(YEARWEEK(fecha, 7), ' Monday'), '%x%v %W') as semana,
    cliente,
    case
      when sum(PriceExclTax) > 0 then 1
      else 0
    end as bebesyninos
  from
    datos
  where
    catid = 74
  group by
    semana,
    cliente
),
cat75 as (
  select
    STR_TO_DATE(CONCAT(YEARWEEK(fecha, 7), ' Monday'), '%x%v %W') as semana,
    cliente,
    case
      when sum(PriceExclTax) > 0 then 1
      else 0
    end as juguetes
  from
    datos
  where
    catid = 75
  group by
    semana,
    cliente
),
cat76 as (
  select
    STR_TO_DATE(CONCAT(YEARWEEK(fecha, 7), ' Monday'), '%x%v %W') as semana,
    cliente,
    case
      when sum(PriceExclTax) > 0 then 1
      else 0
    end as electrodomesticos
  from
    datos
  where
    catid = 76
  group by
    semana,
    cliente
),
cat77 as (
  select
    STR_TO_DATE(CONCAT(YEARWEEK(fecha, 7), ' Monday'), '%x%v %W') as semana,
    cliente,
    case
      when sum(PriceExclTax) > 0 then 1
      else 0
    end as hogarycocina
  from
    datos
  where
    catid = 77
  group by
    semana,
    cliente
),
cat78 as (
  select
    STR_TO_DATE(CONCAT(YEARWEEK(fecha, 7), ' Monday'), '%x%v %W') as semana,
    cliente,
    case
      when sum(PriceExclTax) > 0 then 1
      else 0
    end as belleza
  from
    datos
  where
    catid = 78
  group by
    semana,
    cliente
),
cat79 as (
  select
    STR_TO_DATE(CONCAT(YEARWEEK(fecha, 7), ' Monday'), '%x%v %W') as semana,
    cliente,
    case
      when sum(PriceExclTax) > 0 then 1
      else 0
    end as saludybelleza
  from
    datos
  where
    catid = 79
  group by
    semana,
    cliente
),
zipEU as (
  Select
    *
  From
    (
      select
        Id as AddressId,
        PhoneNumber
      from
        address
    ) as a
    inner join (
      select
        Id as UserId,
        BillingAddress_Id
      from
        customer
    ) as c on a.AddressId = c.BillingAddress_Id
    inner join (
      select
        EntityId,
        `Value` as ZipCode
      From
        genericattribute
      Where
        KeyGroup = 'Customer'
        and genericattribute.Key = 'ZipPostalCode'
    ) as ga on c.UserId = ga.EntityId
)
select
  i.semana,
  n.nacimiento,
  n.ultimopedido,
  CASE
    WHEN datediff(now(), n.ultimoPedido) < 15 THEN 1
    ELSE 0
  END AS activo1,
  i.cliente,
  i.tienda,
  i.ZipCode_tienda,
  i.gmv,
  f.frecuencia,
  t.clientesactivos,
  t.Latitude,
  t.Longitude,
  i.skus,
  i.categorias,
  i.costo,
  i.gm,
  h.skuhistoricos,
  h.catshistoricas,
  (i.gmv) /(f.frecuencia) as AOV,
  t.clientesactivos

from
  infoclientes as i
  inner join nacimientoclientes as n on n.cliente = i.cliente
  inner join frecuenciasem as f on f.cliente = i.cliente
  and f.semana = i.semana
  inner join tiendas as t on t.tienda = i.tienda
  and t.semana = i.semana
  inner join historico as h on h.cliente = i.cliente

group by
  semana,
  cliente"""
  
  