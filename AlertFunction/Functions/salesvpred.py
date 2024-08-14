import psycopg2
import pandas as pd
from AlertFunction.params import prod_params
from AlertFunction.constant import restaurants
from datetime import timedelta

def sales_vs_pred():
    with psycopg2.connect(**prod_params) as conn:
        with conn.cursor() as cursor:
            end_date_query = '''
            SELECT MAX(gastronomic_day)
            FROM public."SalesData"
            WHERE restaurant = %s
        '''
            all_data = pd.DataFrame()
            for restaurant in restaurants:
                cursor.execute(end_date_query,(restaurant,))
                latest_gastronomic_day = cursor.fetchone()[0]
                if latest_gastronomic_day:
                    to_date= latest_gastronomic_day.strftime("%Y-%m-%d")
                    from_date = (latest_gastronomic_day - timedelta(days=6)).strftime("%Y-%m-%d")
                # if restaurant in ['Restaurant','Fisketorget Utsalg']:
                #     from_date = 9
                #     to_date = 2
                # elif restaurant in ['Åsane Storsenter']:
                #     to_date = 2
                #     from_date = 9
                # else:
                #     from_date = 8
                #     to_date = 1
                sales_vs_pred_query = f'''
                WITH LatestEntries AS (
                        SELECT
                            date,
                            restaurant,
                            MAX(created_at) AS latest_created_at
                        FROM
                            public."Predictions_predictions"
                        WHERE
                            date >= '{from_date}'
                            and date <= '{to_date}'
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
                        where p.restaurant = '{restaurant}'
                    group BY
                        p.restaurant),
                    actual_sales as (
                        SELECT restaurant, sum(total_net) as actual_sales
                        FROM 
                            public."SalesData"
                        WHERE
                            gastronomic_day >= '{from_date}'
                            and gastronomic_day <= '{to_date}'
                            and restaurant = '{restaurant}'
                        GROUP BY restaurant
                    )

                    SELECT '{from_date}' as to, '{to_date}' from_date, Ac.restaurant,Ac.actual_sales,Ap.actual_pred
                        FROM actual_sales Ac  join actual_prediction Ap on Ac.restaurant = Ap.restaurant

                '''
                cursor.execute(sales_vs_pred_query)
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