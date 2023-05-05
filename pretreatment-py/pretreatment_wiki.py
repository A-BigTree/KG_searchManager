from tools import *


CONFIG_ = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '020312',
    'database': 'wiki'
}


class Data2Mysql:
    def __init__(self):
        self.conn = DBManager(CONFIG_)

    def label2mysql(self):
        file_line = get_line_iter("F:\\wiki\\latest-truthy.nt.bz2")
        sql = "INSERT INTO q_label (q_id, label) VALUES (%s, %s)"
        data_ = []
        sum_ = 0
        for line in file_line:
            line_pre = token_line_pre(line)
            tokens = line_pre.split(" ")
            if is_token_reg(tokens[0], WikiSchema.Q_REG) and is_token_reg(
                    tokens[1], WikiSchema.LABEL_REG) and is_token_reg(tokens[2], WikiSchema.STR_EN_REG):
                data_.append((token_entity_q(tokens[0]), token_str_en(tokens[2])))
            if len(data_) >= 10000:
                self.conn.add_data(sql, data_)
                sum_ += len(data_)
                logging.info("Add data successfully.Sum:%d." % sum_)
                data_ = []
        self.conn.add_data(sql, data_)
        sum_ += len(data_)
        logging.info("Add data successfully.Sum:%d." % sum_)
        self.conn.db_close()
