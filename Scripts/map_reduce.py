import sys
import re

def mapper():
    try:
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue

            # 分割文本，获取实际内容部分（第二部分）
            parts = line.split('|||')
            if len(parts) >= 2:
                text = parts[1].strip()

                # 使用空格分词，保留中文字符
                # 将所有非中文字符（除了空格）替换为空格
                cleaned_text = re.sub(r'[^\u4e00-\u9fa5\s]', ' ', text)

                # 分词（按空格分割）
                words = cleaned_text.split()

                # 输出词频对
                for word in words:
                    if word:  # 确保不是空字符串
                        print(f"{word}\t1")

    except Exception as e:
        sys.stderr.write(f"Error in mapper: {str(e)}\n")
        sys.exit(1)

def reducer():
    try:
        word_count = {}
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue

            try:
                word, count = line.split("\t")
                count = int(count)
            except ValueError:
                continue

            # 累加词频
            if word in word_count:
                word_count[word] += count
            else:
                word_count[word] = count

        # 按词频排序并输出
        sorted_words = sorted(word_count.items(), key=lambda x: (-x[1], x[0]))
        for word, count in sorted_words:
            print(f"{word}\t{count}")

    except Exception as e:
        sys.stderr.write(f"Error in reducer: {str(e)}\n")
        sys.exit(1)

if __name__ == "__main__":
    try:
        if len(sys.argv) != 2:
            sys.stderr.write("Usage error: Please provide 'mapper' or 'reducer'\n")
            sys.exit(1)

        # 设置输入输出的编码
        import io
        sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

        if sys.argv[1] == "mapper":
            mapper()
        elif sys.argv[1] == "reducer":
            reducer()
        else:
            sys.stderr.write(f"Invalid argument: {sys.argv[1]}. Use 'mapper' or 'reducer'.\n")
            sys.exit(1)

    except Exception as e:
        sys.stderr.write(f"Error in main: {str(e)}\n")
        sys.exit(1)