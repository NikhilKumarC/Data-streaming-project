-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##loading silver table with enriched details

-- COMMAND ----------

create live table checkous_enrich as (
  select
    billing_address,
    checkout_id,
    datetime_occured,
    ip_address,
    payment_method,
    product_id,
    shipping_address,
    total_amount,
    user_agent,
    user_id,
    u.name,
    p.name as product_name,
    p.price as price
  from
    live.checkouts as ck
    left join streamingproject.user as u on ck.user_id = u.id
    left join streamingproject.products as p on ck.product_id = p.id
)

-- COMMAND ----------

create live table clicks_enrich as (
  select
    channel,
    click_id,
    datetime_occured,
    ip_address,
    product,
    product_id,
    url,
    user_agent,
    user_id,
    u.name,
    p.name as product_name,
    p.price as price
  from
    live.clicks as ck
    left join streamingproject.user as u on ck.user_id = u.id
    left join streamingproject.products as p on ck.product_id = p.id
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Loading Golden table

-- COMMAND ----------

-- DBTITLE 1,Finding the first channel that initiated the purchase of a product
create live table first_click_attribute as (
  select
    *
  from
    (
      select
        click.channel,
        click.datetime_occured as click_time,
        click.user_id,
        click.product_id,
        chekout.datetime_occured as checkout_time,
        chekout.product_name,
        chekout.price,
        row_number() over (
          partition by click.user_id,
          click.product_id
          order by
            click.datetime_occured asc
        ) as rnk
      from
        live.checkous_enrich as chekout
        left join live.clicks_enrich as click on chekout.user_id == click.user_id
        AND chekout.product_id = click.product_id
        AND chekout.datetime_occured > click.datetime_occured
    )
  where
    rnk == 1
)

-- COMMAND ----------


