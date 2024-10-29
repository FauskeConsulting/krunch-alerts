import pandas as pd
from AlertFunction.params import params
from AlertFunction.constant import restaurants
import psycopg2
from datetime import date


query = '''
WITH SalesDataWithDifferences AS (
    -- Query to calculate individual differences
    WITH SalesSum AS (
        SELECT 
            gastronomic_day AS sales_date,
            restaurant,
            SUM(total_net) AS total_sales
        FROM public."SalesData"
        WHERE gastronomic_day BETWEEN %s AND %s
        GROUP BY gastronomic_day, restaurant
    ),
    LatestPredictions AS (
        SELECT 
            p1.date AS prediction_date,
            p1.restaurant,
            p1.total_gross AS predicted_sales,
            p1.created_at
        FROM public."Predictions_predictions" p1
        INNER JOIN (
            SELECT 
                date,
                restaurant,
                MAX(created_at) AS latest_created_at
            FROM public."Predictions_predictions"
            WHERE date BETWEEN '2024-10-01' AND '2024-10-22'
            GROUP BY date, restaurant
        ) p2
        ON p1.date = p2.date
        AND p1.restaurant = p2.restaurant
        AND p1.created_at = p2.latest_created_at
    )
    SELECT 
        s.restaurant,
        s.sales_date,
        s.total_sales,
        p.predicted_sales,
        CASE 
            WHEN p.predicted_sales IS NOT NULL AND s.total_sales IS NOT NULL THEN
                ABS((s.total_sales - p.predicted_sales) / p.predicted_sales) * 100
            ELSE NULL
        END AS percentage_difference
    FROM SalesSum s
    LEFT JOIN LatestPredictions p
    ON s.sales_date = p.prediction_date
    AND s.restaurant = p.restaurant
    WHERE p.restaurant = %s
)
-- Calculate average percentage difference
SELECT 
    restaurant,
	sum(total_sales) as sales,
	sum(predicted_sales) as prediction,
    AVG(percentage_difference) AS average_percentage_difference
FROM SalesDataWithDifferences
WHERE percentage_difference IS NOT NULL
GROUP BY 1;
'''



def percentage_diff_per_month():
    with psycopg2.connect(**params) as conn:
        with conn.cursor() as cursor:
            all_data = pd.DataFrame()
            # Get today's date
            today = date.today()
            # Replace the day with 1
            first_of_month = today.replace(day=1)
            for restaurant in restaurants:
                cursor.execute(query,[first_of_month,today,restaurant])
                rows = cursor.fetchall()
                temp_df = pd.DataFrame(rows, columns=['Restaurant', 'sales','prediction','Average of "%" difference per day'])
                all_data= pd.concat([all_data,temp_df],ignore_index= True)
    all_data.reset_index(drop=True, inplace=True)
    # all_data['Average "%" differrence per month'] = all_data['Average "%" differrence per month'],2)
    all_data.insert(0,'s.n', range(1, len(all_data) + 1))
    all_data['Average of "%" difference per day'] = all_data['Average of "%" difference per day'].apply(lambda x: round(x,2))
    all_data['From'] = f'{first_of_month}'
    all_data['To'] = f'{today}'
    all_data = all_data[['From','To','sales','prediction','Restaurant','Average of "%" difference per day']]
    conn.close()
    return all_data