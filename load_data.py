import psycopg2
import pickle
from collections import defaultdict


def dict_factory():
    return defaultdict(dict_factory)


class LoadSqlData:
    """
    从数据库中读取需要的站点信息，站点特征，以及污染物的值
    """

    def __init__(self, username="feature", password="101featureAOIFJBQW1ASasFDA1OFT", database="machine_learning_feature", port="1433",
                 dialect="postgres", host="pgm-wz9gmqmrz445e49v5o.pg.rds.aliyuncs.com"):
        self.username = username
        self.password = password
        self.database = database
        self.port = port
        self.dialect = dialect
        self.host = host
        self.conn = psycopg2.connect(database=self.database, user=self.username, password=self.password,
                                     host=self.host, port=self.port)
        self.cur = self.conn.cursor()
        self.stations_dynamic_feature = dict_factory()
        self.stations = defaultdict(dict)

    def get_data(self):
        self.get_station_info()
        self.get_dynamic_feature()
        self.get_air_quality()
        self.get_weather_feature()
        return self.stations, self.stations_dynamic_feature

    def get_station_info(self):
        """
        从数据库中读取国控站的位置和id信息，和静态特征
        """
        self.cur.execute("select id, grid_id, coord from air_quality_station where grid_id is not null")
        stations = defaultdict(dict)
        row = self.cur.fetchone()
        while row:
            stations[row[1]]['id'] = row[0]
            stations[row[1]]['coord'] = row[2]
            row = self.cur.fetchone()
        self.stations = stations
        self.get_static_feature()

    def get_dynamic_feature(self):
        """
        从数据库中读取国控站的动态特征
        """
        self.cur.execute("select grid_id, data, published_at from grid_dynamic_feature")
        row = self.cur.fetchone()
        while row:
            data_time = row[2]
            grid_id = row[0]
            if grid_id in self.stations:
                self.stations_dynamic_feature[data_time][self.stations[grid_id]['id']]['dynamic_feature'] = row[1]
            row = self.cur.fetchone()

    def get_static_feature(self):
        """
        从数据库中读取国控站的静态特征
        """
        self.cur.execute("select grid_id, data from grid_static_feature")
        row = self.cur.fetchone()
        while row:
            grid_id = row[0]
            data = row[1]
            if grid_id in self.stations:
                self.stations[grid_id]['static_feature'] = data
            row = self.cur.fetchone()

    def get_air_quality(self):
        self.cur.execute("select grid_id, data, published_at from station_air_quality")
        row = self.cur.fetchone()
        while row:
            data_time = row[2]
            grid_id = row[0]
            if grid_id in self.stations:
                self.stations_dynamic_feature[data_time][self.stations[grid_id]['id']]['air_quality'] = row[1]
            row = self.cur.fetchone()

    def get_weather_feature(self):
        """
        从数据库中读取全局的天气信息
        :return:
        """
        self.cur.execute("select data, published_at from station_hour_weather where station_code='CN101270101'")
        row = self.cur.fetchone()
        while row:
            data_time = row[1]
            if data_time in self.stations_dynamic_feature:
                for id in self.stations_dynamic_feature[data_time]:
                    self.stations_dynamic_feature[data_time][id]['weather'] = row[0]
            row = self.cur.fetchone()


if __name__ == '__main__':
    data = LoadSqlData()
    stations, stations_dynamic_feature = data.get_data()
    data = {"stations": stations, "stations_dynamic_feature": stations_dynamic_feature}
    with open('data.pickle', 'wb') as f:
        pickle.dump(data, f)



    # with open('data.pickle', 'rb') as f:
    #     data = pickle.load(f)
    #     data



