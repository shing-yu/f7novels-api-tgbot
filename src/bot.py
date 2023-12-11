import os
import sys
import re
# noinspection PyPackageRequirements
import telebot
from loguru import logger
import json
import sqlite3
from datetime import datetime, timedelta

import queue
import threading
import multiprocessing
from multiprocessing import Pool
import time
from fanqie_api import download, update

with open("config.json", "r", encoding='utf-8') as conf:
    try:
        config = json.load(conf)
    except json.JSONDecodeError as conf_e:
        raise json.JSONDecodeError("配置文件格式不正确", conf_e.doc, conf_e.pos)

os.makedirs(config["save_dir"], exist_ok=True)

try:
    start_hour = int(config["time_range"].split("-")[0])
    end_hour = int(config["time_range"].split("-")[1])
except ValueError:
    pass

logger.remove()
logger.add(config["log"]["filepath"], rotation=config["log"]["maxSize"], level=config["log"]["level"],
           retention=config["log"]["backupCount"], encoding="utf-8", enqueue=True)
logger.add(sys.stdout, level=config["log"]["console_level"], enqueue=True)

BOT_TOKEN = config["bot_token"]


# 创建并连接数据库
db = sqlite3.connect(config["database"], check_same_thread=False)
logger.debug("数据库连接成功")

# 创建一个黑名单表
db.execute('''
CREATE TABLE IF NOT EXISTS blacklist
(chat_id TEXT PRIMARY KEY,
unblock_time TEXT);
''')

# 创建一个任务状态表
db.execute('''
CREATE TABLE IF NOT EXISTS novels
(id TEXT PRIMARY KEY,
name TEXT,
status TEXT,
last_cid TEXT,
last_update TEXT,
finished INTEGER
chat_id INTEGER);
''')

bot = telebot.TeleBot(BOT_TOKEN)


@bot.message_handler(commands=['start'])
def send_welcome(message):
    bot.send_message(message.chat.id, """欢迎使用此机器人
此机器人用于下载番茄和七猫的小说
(七猫暂未实现，敬请期待)
请使用 /help 命令查看帮助
""")


@bot.message_handler(commands=['help'])
def send_help(message):
    bot.send_message(message.chat.id, f"""使用方法：
添加下载任务: /add + 链接或ID
查看所有下载任务: /query 
查看指定下载任务: /query + 链接或ID
下载已完成的任务: /download + 链接或ID
搜索已完成的小说: /name + 小说名
列出你的历史记录: /my
清除你的历史记录: /clear
e.g. /add 123456

编码格式: {config["encoding"]}
下载完成后会自动发送文件

机器人目前仅支持txt格式
如果需要下载epub格式，请使用电脑版程序下载
""")
    # 支持的编码格式：gbk、gb2312、utf-8
    #  + 编码格式（可选）


# 预处理发送的选项
@bot.message_handler(commands=['add', 'query', 'download'])
def preprocessing(message):
    logger.info(f"ChatID: {message.chat.id} 发送了命令: {message.text}")
    # 获取消息内容
    msg = message.text.split()
    category = msg[0].replace("/", "")
    url_id = None
    if category != "download":
        # 判断是否在限时范围内
        now = datetime.utcnow() + timedelta(hours=8)
        if config["time_range"] == "false":
            logger.debug("未设置时间范围")
            pass
        else:
            if not (start_hour <= now.hour < end_hour):
                logger.debug(f"当前时间: {now.hour}点，不在时间范围内")
                bot.send_message(message.chat.id, f"此服务只在{start_hour}点到{end_hour}点开放。")
                return
            logger.debug(f"当前时间: {now.hour}点，请求通过")
    if category == "add":
        # 如果消息内容小于2或大于3，说明消息格式不正确
        # if len(msg) < 2 or len(msg) > 3:
        if len(msg) != 2:
            bot.send_message(message.chat.id, "消息格式不正确，请使用 /help 命令查看帮助，注意空格")
            return
        else:
            # 获取链接或ID
            url_id = msg[1]
    elif category == "query":
        # 如果消息内容小于1或大于2，说明消息格式不正确
        if len(msg) == 1:
            query_all(message.chat.id)
            return
        elif len(msg) == 2:
            # 获取链接或ID
            url_id = msg[1]
        else:
            bot.send_message(message.chat.id, "消息格式不正确，请使用 /help 命令查看帮助，注意空格")
            return
    elif category == "download":
        if len(msg) != 2:
            bot.send_message(message.chat.id, "消息格式不正确，请使用 /help 命令查看帮助，注意空格")
            return
        else:
            # 获取链接或ID
            url_id = msg[1]

    # 获取链接或ID
    if url_id.isdigit():
        logger.debug(f"ID: {url_id} 是纯数字，将被直接使用")
        book_id = url_id
        pass
    else:
        if 'fanqienovel.com/page' in url_id:
            logger.debug("用户发送了PC端目录页的链接，将被转换为ID")
            # noinspection PyBroadException
            try:
                book_id = re.search(r"page/(\d+)", url_id).group(1)
            except Exception:
                logger.info("用户发送的链接转换失败")
                bot.send_message(message.chat.id, "你发送的不是书籍ID或正确的链接。")
                return
        elif 'changdunovel.com' in url_id:
            logger.debug("用户发送了移动端分享链接")
            # noinspection PyBroadException
            try:
                book_id = re.search(r"book_id=(\d+)&", url_id).group(1)
            except Exception:
                logger.info("用户发送的链接转换失败")
                bot.send_message(message.chat.id, "你发送的不是书籍ID或正确的链接。")
                return
        else:
            logger.info("用户发送的内容无法识别")
            bot.send_message(message.chat.id, "你发送的不是书籍ID或正确的链接。")
            return

    if category == "add":
        # 获取编码格式
        # try:
        #     encoding = msg[2]
        # except IndexError:
        #     encoding = "utf-8"
        # # 如果编码格式不在列表中，说明编码格式不正确
        # if encoding not in ["gbk", "gb2312", "utf-8"]:
        #     bot.send_message(message.chat.id, "编码格式不正确，请使用 /help 命令查看帮助")
        #     return

        add_task(book_id, message.chat.id)
    elif category == "query":
        query_task(book_id, message.chat.id)
    elif category == "download":
        download(book_id, message.chat.id)


def add_task(book_id: str,  chat_id: int):
    try:
        data = {
            "action": "add",
            "id": book_id,
        }
        res = api(data, chat_id)
        if res["message"] == "此书籍已添加到下载队列":
            bot.send_message(chat_id, f"恭喜，此书籍已成功添加到下载队列\n"
                                      f"书籍ID: {book_id}\n"
                                      f"位置: {res['position']}\n"
                                      f"状态: {res['status']}")
        elif res["message"] == "finished":
            keyboard = telebot.types.InlineKeyboardMarkup()
            button = telebot.types.InlineKeyboardButton("点击下载", callback_data=book_id)
            keyboard.add(button)
            bot.send_message(chat_id, f"此书籍已存在且已完结，请点击下方按钮下载", reply_markup=keyboard)
        else:
            bot.send_message(chat_id, f"{res['message']}\n"
                                      f"书籍ID: {book_id}\n"
                                      f"位置: {res['position']}\n"
                                      f"状态: {res['status']}\n"
                                      f"上次更新: {res['last_update']}")

    except BaseException as e:
        # 如果发生异常，发送异常信息
        bot.send_message(chat_id, f"添加任务失败：{e}")


def query_task(book_id: str,  chat_id: int):
    try:
        data = {
            "action": "query",
            "id": book_id,
        }
        res = api(data, chat_id)
        if res["exists"] is False:
            bot.send_message(chat_id, f"此书籍不存在\n"
                                      f"书籍ID: {book_id} ")
        else:
            bot.send_message(chat_id, f"状态:{res['status']}\n"
                                      f"书籍ID: {book_id}\n"
                                      f"位置: {res['position']}\n"
                                      f"上次更新: {res['last_update']}")

    except BaseException as e:
        # 如果发生异常，发送异常信息
        bot.send_message(chat_id, f"查询失败：{e}")


def query_all(chat_id: int):
    curb = db.cursor()
    curb.execute("SELECT id, status FROM novels WHERE status IN (?, ?, ?) ORDER BY ROWID",
                 ("进行中", "等待中", "等待更新中"))
    rows = curb.fetchall()
    curb.close()
    if len(rows) == 0:
        bot.send_message(chat_id, "没有未完成的任务")
    else:
        tasks = ""
        for row in rows:
            tasks += f"ID: {row[0]} 状态: {row[1]}\n"
        bot.send_message(chat_id, tasks)


def download(book_id, chat_id):
    curd = db.cursor()
    curd.execute("SELECT name FROM novels WHERE id=? AND status NOT IN ('失败', '进行中', '等待中')", (book_id, ))
    row = curd.fetchone()
    curd.close()
    if row is None:
        bot.send_message(chat_id, f"抱歉，你想要下载的小说不存在。\n"
                                  f"请检查你的链接或ID是否正确，或者稍后再试。")
        return
    title = row[0]
    file_path = os.path.join(config["save_dir"],
                             config["filename_format"].format(title=title, book_id=book_id))
    try:
        with open(file_path, "rb") as f:
            bot.send_message(chat_id, text="正在发送，请稍等...")
            bot.send_document(chat_id, f)
    except FileNotFoundError:
        bot.send_message(chat_id, f"抱歉，未找到小说文件。\n"
                                  f"文件不存在，请向管理员反馈。")


@bot.message_handler(commands=['name'])
def name_search(message):
    msg = message.text.split()
    if len(msg) != 2:
        bot.send_message(message.chat.id, "消息格式不正确，请使用 /help 命令查看帮助，注意空格")
        return
    name = msg[1]
    curn = db.cursor()
    curn.execute("SELECT id, name FROM novels WHERE name LIKE ? AND status NOT IN ('失败', '进行中', '等待中')", (f"%{name}%",))
    rows = curn.fetchall()
    curn.close()
    if len(rows) == 0:
        bot.send_message(message.chat.id, "没有找到相关小说")
    else:
        # 使用按钮请用户选择
        keyboard = telebot.types.InlineKeyboardMarkup()
        for row in rows:
            button = telebot.types.InlineKeyboardButton(row[1], callback_data=row[0])
            keyboard.add(button)
        bot.send_message(message.chat.id, "请选择你要下载的小说：", reply_markup=keyboard)


@bot.callback_query_handler(func=lambda call: True)
def callback_query(call):
    bot.answer_callback_query(call.id, "正在发送，请稍候...")
    bot.send_message(call.message.chat.id, text="正在发送，请稍等...")
    download(call.data, call.message.chat.id)


@bot.message_handler(commands=['my'])
def my_history(message):
    curh = db.cursor()
    curh.execute("SELECT id, name FROM novels WHERE chat_id=? AND status NOT IN ('失败', '进行中', '等待中') ORDER BY ROWID",
                 (message.chat.id,))
    rows = curh.fetchall()
    curh.close()
    if len(rows) == 0:
        bot.send_message(message.chat.id, "没有找到你曾经下载完成的小说")
    else:
        text = ""
        # 使用按钮请用户选择
        keyboard = telebot.types.InlineKeyboardMarkup()
        for row in rows:
            text += f"ID: {row[0]} 名称: {row[1]}\n"
            button = telebot.types.InlineKeyboardButton(row[1], callback_data=row[0])
            keyboard.add(button)
        text += "请点击下方按钮下载"
        bot.send_message(message.chat.id, text, reply_markup=keyboard)


@bot.message_handler(commands=['clear'])
def clear_history(message):
    curc = db.cursor()
    curc.execute("UPDATE novels SET chat_id=NULL WHERE chat_id=?", (message.chat.id,))
    db.commit()
    curc.close()
    bot.send_message(message.chat.id, "已清除你的下载历史记录")


def book_id_to_url(book_id):
    return 'https://fanqienovel.com/page/' + book_id


def url_to_book_id(url):
    return re.search(r"page/(\d+)", url).group(1)


# 定义爬虫类
class Spider:
    def __init__(self):
        # 初始化URL队列
        self.url_queue = queue.Queue()
        # 设置运行状态为True
        self.is_running = True

    @staticmethod
    def crawl(url):
        try:
            logger.info(f"Crawling for URL: {url}")
            book_id = url_to_book_id(url)
            curm = db.cursor()
            curm.execute("SELECT finished, chat_id FROM novels WHERE id=?", (book_id,))
            row = curm.fetchone()
            chat_id = row[1]
            # 根据完结信息判断模式
            if row is not None and row[0] == 0:
                # 如果已有信息，使用增量更新模式
                with Pool(processes=1) as pool:
                    logger.info(f"ID:{book_id} 使用增量更新模式")
                    curm.execute("SELECT name, last_cid FROM novels WHERE id=?", (book_id,))
                    row = curm.fetchone()
                    title = row[0]
                    last_cid = row[1]
                    file_path = os.path.join(config["save_dir"],
                                             config["filename_format"].format(title=title, book_id=book_id))
                    logger.debug(f"名称: {title} 上次更新章节: {last_cid} 生成路径: {file_path} ID: {book_id} 开始更新")
                    res = pool.apply(update, (url, config["encoding"], last_cid, file_path, config, chat_id))  # 运行函数
                    # 获取任务和小说信息
                    status, last_cid, finished = res
                    # 写入数据库
                    curm.execute("UPDATE novels SET last_cid=?, last_update=?, finished=? WHERE id=?",
                                 (last_cid, datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), finished, book_id))
                    db.commit()
                    curm.close()
                    if status == "completed":
                        return "completed"
                    else:
                        return "failed"
            else:
                # 如果没有或者未成功，则普通下载
                with Pool(processes=1) as pool:
                    logger.info(f"ID:{book_id} 使用普通下载模式")
                    logger.debug(f"ID: {book_id} 开始下载")
                    res = pool.apply(download, (url, config["encoding"], config, chat_id))  # 运行函数
                    # 获取任务和小说信息
                    status, name, last_cid, finished = res
                    # 写入数据库
                    curm.execute("UPDATE novels SET name=?, last_cid=?, last_update=?, finished=? WHERE id=?",
                                 (name, last_cid, datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), finished, book_id))
                    db.commit()
                    curm.close()
                    if status == "completed":
                        return "True"
                    else:
                        return "False"
        except Exception as e:
            print(f"Error: {e}")
            return "False"

    def worker(self):
        # 当运行状态为True时，持续工作
        while self.is_running:
            try:
                # 从URL队列中获取URL
                url = self.url_queue.get(timeout=1)
                book_id = url_to_book_id(url)
                curn = db.cursor()
                logger.debug(f"ID: {book_id} 开始任务")
                curn.execute("UPDATE novels SET status=? WHERE id=?", ("进行中", book_id))
                db.commit()
                logger.debug(f"ID: {book_id} 状态更新为进行中")
                status = Spider.crawl(url)
                # 调用爬虫函数爬取URL，如果出错则标记为失败并跳过这个任务进行下一个
                if status == "True":
                    curn.execute("UPDATE novels SET status=? WHERE id=?", ("已完成", book_id))
                    db.commit()
                    logger.debug(f"ID: {book_id} 状态更新为已完成")
                elif status == "completed":
                    curn.execute("UPDATE novels SET status=? WHERE id=?", ("已更新完成", book_id))
                    db.commit()
                    logger.debug(f"ID: {book_id} 状态更新为已更新完成")
                elif status == "failed":
                    curn.execute("UPDATE novels SET status=? WHERE id=?", ("更新失败", book_id))
                    db.commit()
                    logger.debug(f"ID: {book_id} 状态更新为更新失败")
                else:
                    curn.execute("UPDATE novels SET status=? WHERE id=?", ("失败", book_id))
                    db.commit()
                    logger.debug(f"ID: {book_id} 状态更新为失败")
                curn.close()
                # 完成任务后，标记任务为完成状态
                self.url_queue.task_done()
                logger.debug(f"ID: {book_id} 任务结束 结束状态: {status}")
            except queue.Empty:
                time.sleep(5)
                logger.trace("队列为空，等待5秒")
                continue

    def start(self):
        logger.info("爬虫工作启动")
        # 启动时检查数据库中是否有未完成的任务
        curc = db.cursor()
        curc.execute("SELECT id FROM novels WHERE status IN (?, ?, ?) ORDER BY ROWID",
                     ("进行中", "等待中", "等待更新中"))
        rows = curc.fetchall()
        curc.close()
        if len(rows) == 0:
            logger.success("数据库中没有未完成的任务")
        if len(rows) > 0:
            logger.warning(f"数据库中有{len(rows)}个未完成的任务")
        # 有则添加到队列
        for row in rows:
            self.url_queue.put(book_id_to_url(row[0]))
            logger.debug(f"ID: {row[0]} 已添加到队列")
        # 启动工作线程
        threading.Thread(target=self.worker, daemon=True).start()

    def add_url(self, book_id, chat_id):
        logger.debug(f"尝试添加ID: {book_id} 到队列")
        cura = db.cursor()
        cura.execute("SELECT status, finished FROM novels WHERE id=?", (book_id,))
        row = cura.fetchone()
        if row is None or row[0] == "失败":
            self.url_queue.put(book_id_to_url(book_id))
            logger.debug(f"ID: {book_id} 已添加到队列")
            cura.execute("INSERT OR REPLACE INTO novels (id, status, chat_id) VALUES (?, ?, ?)",
                         (book_id, "等待中", chat_id))
            db.commit()
            cura.close()
            return "此书籍已添加到下载队列"
        else:
            # 如果已存在，检查书籍是否已完结
            if row[1] == 1:
                cura.close()
                logger.debug(f"ID: {book_id} 已存在且已完结")
                # 如果已完结，返回提示信息
                return "finished"
            elif row[0] == "等待中" or row[0] == "进行中" or row[0] == "等待更新中":
                cura.close()
                logger.debug(f"ID: {book_id} 已存在且正在下载")
                # 如果正在下载，返回提示信息
                return "此书籍已存在且正在下载"
            else:
                cura.execute("SELECT last_update FROM novels WHERE id=?", (book_id,))
                row = cura.fetchone()
                last_update = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S.%f')

                # 如果上次时间距现在小于3小时，返回提示
                if datetime.now() - last_update < timedelta(hours=3):
                    cura.close()
                    logger.debug(f"ID: {book_id} 已存在且上次更新距现在不足3小时")
                    return "此书籍已存在且上次更新距现在不足3小时，请稍后再试"

                # 如果未完结，返回提示信息并尝试更新
                self.url_queue.put(book_id_to_url(book_id))
                cura.execute("UPDATE novels SET status=?, chat_id=? WHERE id=?", ("等待更新中", chat_id, book_id))
                db.commit()
                cura.close()
                logger.debug(f"ID: {book_id} 已添加到队列 (等待更新中)")
                return "此书籍已存在，正在尝试更新"

    def stop(self):
        logger.info("爬虫工作暂停")
        # 设置运行状态为False以停止工作线程
        self.is_running = False


if __name__ == '__main__':
    # 创建爬虫实例并启动
    spider = Spider()
    spider.start()


def api(data, chat_id):

    # 如果'action'字段的值为'add'，则尝试将URL添加到队列中，并返回相应的信息和位置
    if data['action'] == 'add':
        logger.debug(f"用户请求添加ID: {data['id']} 到队列")
        book_id = data['id']
        message = spider.add_url(book_id, chat_id)
        url = book_id_to_url(book_id)
        position = list(spider.url_queue.queue).index(url) + 1 if url in list(spider.url_queue.queue) else None
        curq = db.cursor()
        curq.execute("SELECT status, last_update FROM novels WHERE id=?", (book_id,))
        row = curq.fetchone()
        curq.close()
        status = row[0] if row is not None else None
        if row is not None:
            last_update = row[1].split('.')[0] if row[1] is not None else None
        else:
            last_update = None
        logger.debug(f"返回信息: {message} 位置: {position} 状态: {status}")
        return {'message': message, 'position': position, 'status': status, 'last_update': last_update}

    # 如果'action'字段的值为'query'，则检查URL是否在队列中，并返回相应的信息和位置或不存在的信息
    elif data['action'] == 'query':
        logger.debug(f"用户请求查询ID: {data['id']} 的状态")
        book_id = data['id']
        url = book_id_to_url(book_id)
        position = list(spider.url_queue.queue).index(url) + 1 if url in list(spider.url_queue.queue) else None
        curw = db.cursor()
        curw.execute("SELECT status, last_update FROM novels WHERE id=?", (book_id,))
        row = curw.fetchone()
        curw.close()
        status = row[0] if row is not None else None
        if row is not None:
            last_update = row[1].split('.')[0] if row[1] is not None else None
        else:
            last_update = None
        logger.debug(f"返回信息: 状态: {status} 位置: {position}")
        return {'exists': status is not None, 'position': position, 'status': status, 'last_update': last_update}


if __name__ == '__main__':
    multiprocessing.freeze_support()
    bot.infinity_polling()
