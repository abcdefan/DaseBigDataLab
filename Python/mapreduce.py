import sys
import re

def mapper():
    try:
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            
            
            cleaned_line = re.sub(r'[^a-zA-Z0-9\s]', ' ', line)
            
            words = cleaned_line.lower().split()
            
            for word in words:
                
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

            
            if word in word_count:
                word_count[word] += count
            else:
                word_count[word] = count

        
        for word, count in word_count.items():
            print(f"{word}\t{count}")
    
    except Exception as e:
        sys.stderr.write(f"Error in reducer: {str(e)}\n")
        sys.exit(1)

if __name__ == "__main__":
    try:
        if len(sys.argv) != 2:
            sys.stderr.write("Usage error: Please provide 'mapper' or 'reducer'\n")
            sys.exit(1)

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