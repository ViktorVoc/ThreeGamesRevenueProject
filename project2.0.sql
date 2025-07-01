select *
from project.games_paid_users gpu
limit 100  --знайомлюсь з даними 


select *
from project.games_payments gp
limit 100 -- знайомлюсь з даними 


select date_trunc('month', CAST(payment_date AS timestamp)) AS month,
sum(revenue_amount_usd) as MRR
from project.games_payments gp
group by month
order by month -- знаходжу mrr


select sum(revenue_amount_usd) AS total_revenue, -- основний тотал ревеню
count(distinct user_id) AS paying_users, -- кількість юзарів котрі платять без дублікатів
round(sum(revenue_amount_usd) / count(distinct user_id), 2) AS arppu -- знаходжу ARPPU
from project.games_payments gp 

with first_payments as (
  select 
    user_id,
    min(date_trunc('month', payment_date)) as first_payment_month
  from
    project.games_payments
  group by
    user_id ---- тимчасова таблиця знаходить першій місяць платежу
),
new_user_revenue as (
  select
    gp.user_id,
    date_trunc('month', gp.payment_date) as month,
    gp.revenue_amount_usd
  from
    project.games_payments gp ---- тимчасова таблиця всі платіжі зроблені у перший день
  join first_payments fp
    on gp.user_id = fp.user_id
   and date_trunc('month', gp.payment_date) = fp.first_payment_month
)
select
  month,
  count(distinct user_id) as new_paid_users, -- new paid users
  sum(revenue_amount_usd) as new_mrr --new mrr
from
  new_user_revenue
group by
  month
order by
  month;

--churned_revenue/ churned_users
with payments as (
    select
        user_id,
        date_trunc('month', payment_date) as payment_month,
        sum(revenue_amount_usd ) as total_revenue
    from project.games_payments
    group by user_id, date_trunc('month', payment_date)
)

select
    payment_month,
    date(payment_month + interval '1 month') as next_calendar_month,
    lead(payment_month) over (partition by user_id order by payment_month) as next_paid_month,
    case when lead(payment_month) over (partition by user_id order by payment_month)
              is null
          or lead(payment_month) over (partition by user_id order by payment_month) != payment_month + interval '1 month'
         then total_revenue
    end as churned_revenue, --churned_revenue
    case when lead(payment_month) over (partition by user_id order by payment_month)
              is null
          or lead(payment_month) over (partition by user_id order by payment_month) != payment_month + interval '1 month'
         then 1
    end as churned_users -- churned_users
from payments;
--churn rate and revenue_churn_rate
with monthly_payments as (
    select
        user_id,
        date_trunc('month', payment_date) as payment_month,
        sum(revenue_amount_usd) as monthly_revenue
    from project.games_payments
    group by user_id, date_trunc('month', payment_date)
),
churn_analysis as (
  select
    user_id,
    payment_month,
    monthly_revenue,
    lead(payment_month) over (partition by user_id order by payment_month) as next_month
  from monthly_payments
),
churned as (
  select
    payment_month,
    count(user_id) as churned_users,
    sum(monthly_revenue) as churned_revenue
  from churn_analysis
  where next_month is null
     or next_month != payment_month + interval '1 month'
  group by payment_month
),
totals as (
  select
    payment_month,
    count(distinct user_id) as total_users,
    sum(monthly_revenue) as total_revenue
  from monthly_payments
  group by payment_month
)
select
  c.payment_month,
  round(c.churned_users::numeric / nullif(t.total_users,0), 4) as churn_rate,
  round(c.churned_revenue::numeric / nullif(t.total_revenue,0), 4) as revenue_churn_rate
from churned c
join totals t on c.payment_month = t.payment_month;

--Expansion MRR and Contraction MRR
with revenue_by_month as (
    select
        user_id,
        date_trunc('month', payment_date::date) as payment_month,
        sum(revenue_amount_usd) as monthly_revenue
    from games_payments
    group by user_id, payment_month
),
monthly_diff as (
    select
        curr.user_id,
        curr.payment_month,
        curr.monthly_revenue as current_revenue,
        prev.monthly_revenue as previous_revenue,
        curr.monthly_revenue - prev.monthly_revenue as diff
    from revenue_by_month curr
    join revenue_by_month prev
        on curr.user_id = prev.user_id
        and curr.payment_month = prev.payment_month + interval '1 month'
)
select
    payment_month,
    sum(case when diff > 0 then diff else 0 end) as expansion_mrr,
    sum(case when diff < 0 then abs(diff) else 0 end) as contraction_mrr
from monthly_diff
group by payment_month
order by payment_month;

--Lt and LTV
with monthly_payments as (
    select
        user_id,
        date_trunc('month', payment_date) as payment_month,
        sum(revenue_amount_usd) as monthly_revenue
    from project.games_payments
    group by user_id, date_trunc('month', payment_date)
),
churn_analysis as (
  select
    user_id,
    payment_month,
    monthly_revenue,
    lead(payment_month) over (partition by user_id order by payment_month) as next_month
  from monthly_payments
),
churned as (
  select
    payment_month,
    count(user_id) as churned_users,
    sum(monthly_revenue) as churned_revenue
  from churn_analysis
  where next_month is null
     or next_month != payment_month + interval '1 month'
  group by payment_month
),
totals as (
  select
    payment_month,
    count(distinct user_id) as total_users,
    sum(monthly_revenue) as total_revenue
  from monthly_payments
  group by payment_month
),
churn_metrics as (
  select
    c.payment_month,
    round(c.churned_users::numeric / nullif(t.total_users,0), 4) as churn_rate,
    round(c.churned_revenue::numeric / nullif(t.total_revenue,0), 4) as revenue_churn_rate
  from churned c
  join totals t on c.payment_month = t.payment_month
),
arppu_by_month as ( 
  select date_trunc('month', payment_date::date) as month,
  round(sum(revenue_amount_usd)::numeric / nullif(count(distinct user_id), 0), 2) as arppu
  from project.games_payments gp
  group by month
),
ltv as (
  select
    c.payment_month,
    a.arppu,
    round(1 / nullif(c.churn_rate, 0), 2) as lt,
    round(a.arppu * (1 / nullif(c.churn_rate, 0)), 2) as ltv
  from churn_metrics c
  join arppu_by_month a 
    on c.payment_month = a.month
)
select * 
from ltv
order by payment_month
limit 100;
