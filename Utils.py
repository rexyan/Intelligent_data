from fishbase.fish_logger import logger as log
import dask.dataframe as dd

class DataFrameColumnsName(object):
    SET_NEW_COLUMNS_NAME_ERROR = "设置新列名错误"
    ADD_NEW_COLUMNS_NAME_ERROR = "新增新列名错误"

    @staticmethod
    def set_new_columns_name(df, *name) -> dd:
        """
        设置列名称, 这种方法适用于已经有头信息的情况, 进行的是头信息的替换
        :param df:
        :param name:
        :return:
        """
        try:
            df.columns = name
        except Exception as e:
            log.error(f"设置新列名错误: {e}")
            raise Exception(DataFrameColumnsName.SET_NEW_COLUMNS_NAME_ERROR)
        return df

    @staticmethod
    def add_new_columns_name(df, *name) -> dd:
        """
        添加新列名
        :param df:
        :param name:
        :return:
        """
        try:
            current_columns_name = DataFrameColumnsName.get_df_columns_name(df)  # 取到目前的列名
            df.columns = name  # 赋予新的列名

            """
            将列名数据追加到 df 最后, 使其成为其中一行，这样数据才完整，否则会少一列
            1. 创建一个新的 df 对象
            2. 进行合并
            """
            data_dict = dict(zip(name, current_columns_name))
            new_df = dd.DataFrame(data_dict)
            return new_df
        except Exception as e:
            log.error(f"添加新列名错误: {e}")
            raise Exception(DataFrameColumnsName.ADD_NEW_COLUMNS_NAME_ERROR)

    @staticmethod
    def get_df_columns_name(df) -> list:
        """
        获取列名
        :param df:
        :return:
        """
        return df.columns.values
