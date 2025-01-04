import os
import psycopg2
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain.schema.runnable import RunnableLambda, RunnableSequence
from langchain.prompts import ChatPromptTemplate
from langchain.schema.output_parser import StrOutputParser


# Load environment variables
load_dotenv()

# Database configuration
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Create a ChatOpenAI Model
model = ChatOpenAI(model="gpt-4")

# Define prompt templates
prompt_template = ChatPromptTemplate.from_messages(
    [
        ("system", "You are an AI assistant that creates 1 SQL command for your PostgreSQL database based on user queries. There are four tables: books (column name: id, title: string, author: string, year: integer), customer, orders, products. Example query: SELECT COUNT(*) FROM books; Literally only write the SQL command no extra stuff!!!"),
        ("human", "This is my question about the database: {query}"),
    ]
)

# Create individual runnables (steps in the chain)
format_prompt = RunnableLambda(lambda x: prompt_template.format_prompt(**x))
invoke_model = RunnableLambda(lambda x: model.invoke(x.to_messages()))
parse_output = RunnableLambda(lambda x: x.content.strip())  # Clean the response content

# Chain of actions
chain = RunnableSequence(first=format_prompt, middle=[invoke_model], last=parse_output)


def connect_to_db():
    """Establishes connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print("Connected to the database!")
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None


def process_query(conn, query):
    # Generate SQL command from the query using LangChain
    response = chain.invoke({"query": query})
    print(f"Generated SQL: {response}")

    # Extract just the SQL query (removing any extra text)
    
    
    # Execute the SQL query
    with conn.cursor() as cursor:
        try:
            cursor.execute(response)
            if response.strip().lower().startswith("select"):
                results = cursor.fetchall()
                print("Query Results:")
                for row in results:
                    print(row)
            else:
                conn.commit()
                print("Query executed successfully!")
        except Exception as e:
            print(f"Error executing SQL query: {e}")

def extract_sql(response):
    """Extracts the SQL query from the LangChain response."""
    # This assumes that the actual SQL query will be the last line of the response
    lines = response.splitlines()
    # Look for the SQL query in the response
    sql_query = ""
    for line in lines:
        if line.strip().lower().startswith("select") or line.strip().lower().startswith("insert") or line.strip().lower().startswith("update") or line.strip().lower().startswith("delete"):
            sql_query = line.strip()
            break
    return sql_query


def main():
    conn = connect_to_db()
    if not conn:
        return

    print("Welcome to the Command-Line Query System!")
    print("Type 'exit' to quit.")
    
    while True:
        query = input("\nEnter your query: ")
        if query.lower() == "exit":
            break
        process_query(conn, query)

    conn.close()
    print("Connection closed. Goodbye!")


if __name__ == "__main__":
    main()