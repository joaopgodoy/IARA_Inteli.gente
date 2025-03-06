import json, os, regex as re
from .common.functions import get_datapoints_from_database, execute_indicator, save_csv

if __name__ == "__main__":
    initial_dataframe, data_dict = get_datapoints_from_database()

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
                    )

                    save_csv(df=final_dataframe, nome=f"{subdir}_{file}".replace(".py", ""))