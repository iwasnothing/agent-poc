import networkx as nx
from pyvis.network import Network
import matplotlib
matplotlib.use('Agg')  # Set the backend to Agg before importing pyplot
import matplotlib.pyplot as plt
from hashing import hash_query, decode_answer
import pandas as pd
import logging
import declare_constants
logging.basicConfig(level=declare_constants.get_log_level())
logger = logging.getLogger(__name__)

def create_buyer_graph(buyer_data: pd.DataFrame):
    G = nx.Graph()
    for index, row in buyer_data.iterrows():
        decoded_supplier = decode_answer(row['mg_id'])  
        decoded_buyer = decode_answer(row['buyer'])  
        if decoded_buyer not in G.nodes:
            G.add_node(decoded_buyer, country=row['buyer_country'], is_subsid=row['buyer_is_subsid'])
        if decoded_supplier not in G.nodes:
            G.add_node(decoded_supplier)
        G.add_edge(decoded_buyer, decoded_supplier, weight=row['amount']/1000.00, date=row['date'])
    return G

def create_supplier_graph(supplier_data: pd.DataFrame):
    G = nx.Graph()
    for index, row in supplier_data.iterrows():
        decoded_buyer = decode_answer(row['mg_id'])  
        decoded_supplier = decode_answer(row['supplier'])  
        if decoded_supplier not in G.nodes:
            G.add_node(decoded_supplier, country=row['supplier_country'], is_subsid=row['supplier_is_subsid'])
        if decoded_buyer not in G.nodes:
            G.add_node(decoded_buyer)
        G.add_edge(decoded_buyer, decoded_supplier, weight=row['amount']/1000.00, date=row['date'])
    return G

def visualize_graph(G: nx.Graph, filename: str):
    net = Network(height='150px', width='100%',notebook=True)
    net.from_nx(G)
    net.show(filename)

def create_bar_chart(data: pd.DataFrame, column_name: str, filename: str):
    # Force matplotlib to use the Agg backend
    plt.switch_backend('Agg')
    
    data["decoded_"+column_name] = data[column_name].apply(lambda x: decode_answer(x))
    logger.debug(data)
    
    plt.figure(figsize=(10, 6))
    plt.bar(data["decoded_"+column_name], data['amount'], color='skyblue')
    plt.xlabel(column_name)
    plt.ylabel('Amount')
    plt.title(f'Amount by {column_name}')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(filename, format='jpg', dpi=300, bbox_inches='tight')
    plt.close()