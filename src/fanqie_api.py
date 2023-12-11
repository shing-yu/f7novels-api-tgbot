
import os

# 导入必要的模块
import re
from os import path
import time
import public as p
from loguru import logger


# 定义正常模式用来下载番茄小说的函数
def download(url: str, encoding: str, config: dict, chat_id: int) -> tuple:
    from bot import bot
    title = None
    last_cid = None
    finished: int = -1  # 使用数字代表小说是否已完结，-1 代表未知，0 代表未完结，1 代表已完结
    book_id = re.search(r'page/(\d+)', url).group(1)

    # noinspection PyBroadException
    try:

        # 提取书籍ID

        ua = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/118.0.0.0 "
            "Safari/537.36"
        )
        headers, title, content, chapters, finished = p.get_fanqie(url, ua)

        # 定义文件名
        file_path = path.join(config["save_dir"],
                              config["filename_format"].format(title=title, book_id=book_id))

        os.makedirs(config["save_dir"], exist_ok=True)

        last_cid = None

        try:
            # 遍历每个章节链接
            for chapter in chapters:
                time.sleep(config["speed_limit"] if config["speed_limit"] > 0.25 else 0.25)

                result = p.get_api(chapter, headers)

                if result is None:
                    continue
                else:
                    chapter_title, chapter_text, chapter_id = result

                last_cid = chapter_id

                # 在小说内容字符串中添加章节标题和内容
                content += f"\n\n\n{chapter_title}\n{chapter_text}"

                logger.trace(f"ID: {book_id} 已获取 {chapter_title} 章节ID: {chapter_id}")

            # 根据编码转换小说内容字符串为二进制数据
            data = content.encode(encoding, errors='ignore')

            # 保存文件
            with open(file_path, "wb") as f:
                f.write(data)

            logger.success(f"小说《{title}》已保存到本地")

            with open(file_path, "rb") as f:
                bot.send_document(chat_id, f, caption=f"小说下载完成")

            logger.success(f"小说《{title}》已发送到 Telegram")

            status = "completed"

            return status, title, last_cid, finished

        except Exception as e:
            # 捕获所有异常，及时保存文件
            # 根据转换小说内容字符串为二进制数据
            data = content.encode(encoding, errors='ignore')

            # 保存文件
            with open(file_path, "wb") as f:
                f.write(data)

            logger.error(f"小说《{title}》下载失败：{e}")

            logger.exception(e)

            logger.warning(f"小说《{title}》已保存到本地（中断保存）")

            raise Exception(f"下载失败: {e}")

    except Exception:
        bot.send_message(chat_id, f"抱歉，你提交的小说（ID：{book_id}）下载失败。\n"
                                  f"请检查你的链接或ID是否正确，或者稍后再试。\n"
                                  f"（部分小说由于版权原因无法下载）\n")
        return "failed", title, last_cid, finished


def update(url: str, encoding: str, start_id: str, file_path: str, config: dict, chat_id: int) -> tuple:
    from bot import bot
    chapter_id_now = start_id
    finished: int = 0
    book_id = re.search(r'page/(\d+)', url).group(1)

    if os.path.exists(file_path) is False:
        logger.error(f"小说更新失败：本地文件不存在 路径：{file_path}")
        bot.send_message(chat_id, f"抱歉，你提交的小说（ID：{book_id}）更新失败。\n"
                                  f"文件不存在，请向管理员反馈。")
        return "failed", chapter_id_now, finished

    # noinspection PyBroadException
    try:

        ua = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/118.0.0.0 "
            "Safari/537.36"
        )
        headers, title, content, chapters, finished = p.get_fanqie(url, ua)

        last_cid = None
        # 找到起始章节的索引
        start_index = 0
        for i, chapter in enumerate(chapters):
            chapter_url_tmp = chapter.find("a")["href"]
            chapter_id_tmp = re.search(r"/reader/(\d+)", chapter_url_tmp).group(1)
            if chapter_id_tmp == start_id:  # 更新函数，所以前进一个章节
                start_index = i + 1
            last_cid = chapter_id_tmp

        # 判断是否已经最新
        if start_index >= len(chapters):
            logger.info(f"小说《{title}》已经是最新章节，无需更新")
            with open(file_path, "rb") as file:
                bot.send_document(chat_id, file, caption=f"小说已经是最新章节，无需更新")
            return "completed", last_cid, finished

        with open(file_path, 'ab') as f:
            try:
                # 从起始章节开始遍历每个章节链接
                for chapter in chapters[start_index:]:

                    time.sleep(config["speed_limit"] if config["speed_limit"] > 0.25 else 0.25)

                    result = p.get_api(chapter, headers)

                    if result is None:
                        continue
                    else:
                        chapter_title, chapter_text, chapter_id_now = result

                    # 在小说内容字符串中添加章节标题和内容
                    content = f"\n\n\n{chapter_title}\n{chapter_text}"

                    # 根据编码转换小说内容字符串为二进制数据
                    data = content.encode(encoding, errors='ignore')

                    # 将数据追加到文件中
                    f.write(data)

                    logger.debug(f"小说: {title} 已增加 {chapter_title} 章节ID: {chapter_id_now}")

                logger.success(f"小说《{title}》已保存到本地，路径：{file_path}")

                with open(file_path, "rb") as file:
                    bot.send_document(chat_id, file, caption="小说更新完成")

                logger.success(f"小说《{title}》已发送到 Telegram")

                status = "completed"

                return status, chapter_id_now, finished

            except Exception as e:

                logger.error(f"小说《{title}》更新失败：{e}")

                logger.exception(e)

                logger.warning(f"小说《{title}》已保存到本地（中断保存）")

                raise Exception(f"更新失败: {e}")

    except Exception:
        bot.send_message(chat_id, f"抱歉，你提交的小说（ID：{book_id}）更新失败。\n"
                                  f"请尝试稍后再试。")
        return "failed", chapter_id_now, finished
