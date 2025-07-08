WITH monthly_revenue AS (
  SELECT
    user_id,
    date_trunc('month', payment_date) AS payment_month,
    SUM(revenue_amount_usd) AS total_revenue
  FROM project.games_payments
  GROUP BY user_id, date_trunc('month', payment_date)
),
revenue_lag_lead_months AS (
  SELECT
    user_id,
    payment_month,
    total_revenue,
    payment_month - INTERVAL '1 month'::interval AS previous_calendar_month,
    payment_month + INTERVAL '1 month'::interval AS next_calendar_month,
    LAG(total_revenue) OVER (PARTITION BY user_id ORDER BY payment_month) AS previous_paid_month_revenue,
    LAG(payment_month) OVER (PARTITION BY user_id ORDER BY payment_month) AS previous_paid_month,
    LEAD(payment_month) OVER (PARTITION BY user_id ORDER BY payment_month) AS next_paid_month
  FROM monthly_revenue
),
revenue_metrics AS (
  SELECT
    payment_month,
    user_id,
    total_revenue,

    CASE 
      WHEN previous_paid_month IS NULL THEN total_revenue
      ELSE 0
    END AS new_mrr,

    CASE 
      WHEN previous_paid_month IS NOT NULL AND total_revenue > previous_paid_month_revenue THEN total_revenue - previous_paid_month_revenue
      ELSE 0
    END AS expansion_revenue,

    CASE 
      WHEN previous_paid_month IS NOT NULL AND total_revenue < previous_paid_month_revenue THEN previous_paid_month_revenue - total_revenue
      ELSE 0
    END AS contraction_revenue,

    CASE 
      WHEN previous_paid_month IS NOT NULL AND previous_paid_month != previous_calendar_month THEN total_revenue
      ELSE 0
    END AS back_from_churn_revenue,

    CASE 
      WHEN next_paid_month IS NULL OR next_paid_month != next_calendar_month THEN total_revenue
      ELSE 0
    END AS churned_revenue,

    CASE
      WHEN next_paid_month IS NULL OR next_paid_month != next_calendar_month THEN 1
      ELSE 0
    END AS churn_month

  FROM revenue_lag_lead_months
)
SELECT
  rm.*,
  gpu.game_name,
  gpu.language,
  gpu.has_older_device_model,
  gpu.age
FROM revenue_metrics rm
LEFT JOIN project.games_paid_users gpu USING(user_id);
