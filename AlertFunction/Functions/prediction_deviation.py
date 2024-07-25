import psycopg2
import pandas as pd
from AlertFunction.params import prod_params
from AlertFunction.constant import restaurants

def deviation_in_prediction():
    with psycopg2.connect(**prod_params) as conn:
        with conn.cursor() as cursor:
            all_data = pd.DataFrame()
            for restaurant in restaurants:
                deviation_query = '''
                WITH PredictedData AS (
                        SELECT 
                            DISTINCT ON (date, restaurant)
                            id,
                            date,
                            restaurant,
                            total_gross,
                            created_at,
                            company,
                            parent_restaurant,
                            percentage_difference,
                            LAG(total_gross) OVER (PARTITION BY date, restaurant ORDER BY created_at) AS previous_total_gross,
                            FIRST_VALUE(total_gross) OVER (PARTITION BY date, restaurant ORDER BY created_at) AS first_total_gross,
                            LAST_VALUE(total_gross) OVER (PARTITION BY date, restaurant ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_total_gross
                        FROM
                            public."Predictions_predictions"
                        ORDER BY
                            date, restaurant, created_at
                    ),
                    DailyPercentageChange AS (
                        SELECT
                            date,
                            restaurant,
                            first_total_gross,
                            last_total_gross,
                            CASE
                                WHEN first_total_gross IS NULL OR last_total_gross IS NULL OR first_total_gross = 0 THEN NULL
                                ELSE ROUND(((last_total_gross - first_total_gross) / first_total_gross) * 100, 2)
                            END AS overall_percentage_change
                        FROM
                            PredictedData
                        WHERE
                            restaurant = %s
                            AND date > current_date
                            AND date < current_date + interval '15 days'
                        ORDER BY
                            date DESC
                    ),
                    OverallTrend AS (
                        SELECT
                            CASE
                                WHEN AVG(overall_percentage_change) IS NULL THEN 'No Data'
                                WHEN AVG(overall_percentage_change) > 0 THEN 'Increasing'
                                WHEN AVG(overall_percentage_change) < 0 THEN 'Decreasing'
                                ELSE 'Stable'
                            END AS overall_trend
                        FROM
                            DailyPercentageChange
                    )

                    SELECT
                        date,
                        restaurant,
                        first_total_gross,
                        last_total_gross,
                        overall_percentage_change::TEXT AS overall_percentage_change
                    FROM
                        DailyPercentageChange

                    UNION ALL

                    SELECT
                        NULL AS date,
                        %s AS restaurant,
                        NULL AS first_total_gross,
                        NULL AS last_total_gross,
                        overall_trend AS overall_percentage_change
                    FROM
                        OverallTrend
                    ORDER BY
                        date DESC NULLS LAST;
            '''
                cursor.execute(deviation_query,[restaurant,restaurant])
                rows = cursor.fetchall()
                temp_df = pd.DataFrame(rows, columns=['date', 'restaurant', 'first_total_gross', 'last_total_gross', 'overall_percentage_change'])
                # temp_df['date']= temp_df['date'].fillna(restaurant)
                all_data= pd.concat([all_data,temp_df],ignore_index= True)
                # filtered_df = all_data[all_data['date'].isna() & all_data['restaurant'].notna()][['restaurant', 'pred_trend']]

    conn.close()
    return all_data