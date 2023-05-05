import re

from pymysql.connections import Connection
from smart_open import smart_open
import logging
import pymysql

# Logging Config
logging.basicConfig(format="[%(asctime)s][%(levelname)s][%(threadName)s][func:%(funcName)s]-> [%(message)s]",
                    level=logging.INFO)


def get_line_iter(file: str, batch: tuple[int, int] = None) -> str:
    """Get line iter from a big file."""
    try:
        with smart_open(file) as fn:
            logging.info("File:(%s) open successfully." % file)
            if batch is not None:
                logging.info("Iter batch line:(%d -> %d)." % (batch[0], batch[1]))
            index = -1
            for line in fn:
                index += 1
                if index % 10000 == 0:
                    logging.info("Have read line:%d" % index)
                if batch is not None:
                    if index < batch[0]:
                        continue
                    if index >= batch[1]:
                        break
                yield line.decode(encoding='utf-8')
    except Exception as e:
        logging.error("File:(%s) read error." % file)
        logging.error("Error line:%d." % index)
        logging.exception(e)


def token_line_pre(line: str) -> str:
    """Pretreatment for token line."""
    res_line = line.strip()
    res_line = res_line.strip('\n')
    res_line = res_line.strip('.')
    return res_line.strip()


class WikiSchema:
    Q_REG = re.compile(r"^<http://www.wikidata.org/entity/Q\d+>$")
    LABEL_REG = re.compile(r"^<http://schema.org/name>$")
    STR_EN_REG = re.compile(r"@en$")
    P_REG = re.compile(r"^<http://www.wikidata.org/prop/direct/P\d+>$")


def is_token_reg(token: str, reg: re.Pattern) -> bool:
    if len(reg.findall(token)) > 0:
        return True
    return False


def token_entity_q(token: str) -> int:
    token = token.strip("<").strip(">")
    return int(token.replace("http://www.wikidata.org/entity/Q", ""))


def token_entity_p(token: str) -> int:
    token = token.strip("<").strip(">")
    return int(token.replace("http://www.wikidata.org/prop/direct/P", ""))


def token_str_en(token: str) -> str:
    s_ = token.replace("@en", "")
    return s_.strip('"')


class DBManager:
    def __init__(self, config=None):
        self.db: Connection | None = None
        if isinstance(config, dict):
            self.db_config(config)

    def db_config(self, config_dic: dict):
        try:
            self.db = pymysql.connect(host=config_dic['host'],
                                      port=config_dic['port'],
                                      user=config_dic['user'],
                                      password=config_dic['password'],
                                      database=config_dic['database'])
            logging.info("Database:%s connect successfully." % str(self.db.db))
        except Exception as e:
            logging.error("Database connect error.")
            logging.exception(e)

    def db_close(self):
        if self.db is not None:
            self.db.close()

    def add_data(self, sql: str, data: list[tuple]):
        cursor = self.db.cursor()
        try:
            cursor.executemany(sql, data)
            self.db.commit()
            logging.info("Add data:%d successfully." % len(data))
        except Exception as e:
            self.db.rollback()
            logging.error("Database add error.")
            logging.exception(e)
        finally:
            cursor.close()
