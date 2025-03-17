import json, os, regex as re
from .common.utils import get_datapoints_from_database, execute_indicator, save_csv
from .common.insert_db import insert_df_indicators_table
from .common import DBconnection

if __name__ == "__main__":
    try:
        conn = DBconnection.get_connection()
        with DBconnection.get_cursor() as cursor:

            initial_dataframe, data_dict = get_datapoints_from_database(cursor)
            cursor.close()

            with open("indicadores/dimensions.json", "r") as json_file:
                data_list = json.load(json_file)

                pattern = re.compile(r"^\d{4}\.py$")

                for subdir, dirs, files in os.walk('indicadores/'):
                    for file in files:
                        if pattern.match(file):
                            final_dataframe = execute_indicator(
                                data_list=data_list,
                                path=f"{subdir}/{file}",
                                df=initial_dataframe,
                                indicator_datapoints=data_dict
                            ).reset_index()

                            insert_df_indicators_table(
                                df=final_dataframe,
                                has_indicator_score=True
                            )

                            print(f"{subdir}/{file} Succeeded!")
                        
        conn.commit()
    
    except Exception as e:
        conn.rollback()
        print(e)

    finally:
        conn.close()