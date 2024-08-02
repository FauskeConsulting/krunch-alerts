import psycopg2
import pandas as pd
from AlertFunction.params import prod_params
from AlertFunction.constant import restaurants

def sales_vs_pred():
    with psycopg2.connect(**prod_params) as conn:
        with conn.cursor() as cursor:
            all_data = pd.DataFrame()
            for restaurant in restaurants:
                if restaurant in ['Restaurant','Fisketorget Utsalg']:
                    from_date = 9
                    to_date = 2
                else:
                    from_date = 8
                    to_date = 1
                sales_vs_pred_query = f'''
                WITH LatestEntries AS (
                        SELECT
                            date,
                            restaurant,
                            MAX(created_at) AS latest_created_at
                        FROM
                            public."Predictions_predictions"
                        WHERE
                            date > date(CURRENT_DATE - INTERVAL '{from_date} days')
                            and date <= date(CURRENT_DATE - INTERVAL '{to_date} days')
                        GROUP BY
                            date, restaurant
                    ),
                    actual_prediction as (
                    SELECT
                        p.restaurant as restaurant,
                        sum(p.total_gross) as actual_pred
                    FROM
                        public."Predictions_predictions" p
                    JOIN
                        LatestEntries le
                        ON p.date = le.date
                        AND p.restaurant = le.restaurant
                        AND p.created_at = le.latest_created_at
                        where p.restaurant = %s
                    group BY
                        p.restaurant),
                    actual_sales as (
                        SELECT restaurant, sum(total_net) as actual_sales
                        FROM 
                            public."SalesData"
                        WHERE
                            gastronomic_day > date(CURRENT_DATE - INTERVAL '{from_date} days')
                            and gastronomic_day <= date(CURRENT_DATE - INTERVAL '{to_date} days')
                            and restaurant = %s
                        GROUP BY restaurant
                    )

                    SELECT date(CURRENT_DATE - INTERVAL '{from_date} days') as to, date(CURRENT_DATE - INTERVAL '{to_date} days') as from_date, Ac.restaurant,Ac.actual_sales,Ap.actual_pred
                        FROM actual_sales Ac  join actual_prediction Ap on Ac.restaurant = Ap.restaurant

                '''
                cursor.execute(sales_vs_pred_query,[restaurant,restaurant])
                rows = cursor.fetchall()
                temp_df = pd.DataFrame(rows, columns=['from', 'to', 'restaurant', 'Sum of Sales', 'Sum of Prediction'])
                temp_df['Percentage_change'] = ((temp_df['Sum of Sales'] - temp_df['Sum of Prediction'])/temp_df['Sum of Sales'])*100
                temp_df['Percentage_change'] = temp_df['Percentage_change'].astype(float)
                temp_df['Percentage_change'] = round(temp_df['Percentage_change'],2)
                # temp_df['date']= temp_df['date'].fillna(restaurant)
                all_data= pd.concat([all_data,temp_df],ignore_index= True)
                # filtered_df = all_data[all_data['date'].isna() & all_data['restaurant'].notna()][['restaurant', 'pred_trend']]

    conn.close()
    return all_data