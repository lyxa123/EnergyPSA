# Power System Analysis (PSA) Experiment using PyPSA
# This script creates and populates a SQLite database with power system components
# The database will be used for power flow analysis and optimization

import sqlite3

# Database configuration
DB_NAME = 'power_grid.db'

# Sample power system components
# Buses represent nodes in the power system where components connect
# - name: Unique identifier for the bus
# - v_nom_kv: Nominal voltage in kilovolts
# - type: 'b' for regular bus
# - x_coord, y_coord: Geographical coordinates for visualization
sample_buses = [
    {"name": "Substation_A", "v_nom_kv": 110.0, "type": "b", "x_coord": 0.0, "y_coord": 0.0},
    {"name": "City_Center", "v_nom_kv": 110.0, "type": "b", "x_coord": 5.0, "y_coord": 2.0},
    {"name": "Industrial_Park", "v_nom_kv": 110.0, "type": "b", "x_coord": 8.0, "y_coord": -1.0},
    {"name": "Rural_Town", "v_nom_kv": 110.0, "type": "b", "x_coord": -3.0, "y_coord": 4.0},
    {"name": "Wind_Farm_Connection", "v_nom_kv": 110.0, "type": "b", "x_coord": -6.0, "y_coord": -2.0},
]

# Transmission lines connecting buses
# - name: Unique identifier for the line
# - from_bus_name, to_bus_name: Names of connected buses
# - length_km: Line length in kilometers
# - r_ohm_per_km: Resistance per kilometer in ohms
# - x_ohm_per_km: Reactance per kilometer in ohms
# - s_nom_mva: Nominal apparent power capacity in MVA
sample_lines = [
    {"name": "Line_A_City", "from_bus_name": "Substation_A", "to_bus_name": "City_Center", "length_km": 20.0, "r_ohm_per_km": 0.1, "x_ohm_per_km": 0.3, "s_nom_mva": 150.0},
    {"name": "Line_City_Industrial", "from_bus_name": "City_Center", "to_bus_name": "Industrial_Park", "length_km": 15.0, "r_ohm_per_km": 0.15, "x_ohm_per_km": 0.25, "s_nom_mva": 120.0},
    {"name": "Line_A_Rural", "from_bus_name": "Substation_A", "to_bus_name": "Rural_Town", "length_km": 30.0, "r_ohm_per_km": 0.08, "x_ohm_per_km": 0.2, "s_nom_mva": 180.0},
    {"name": "Line_Rural_Wind", "from_bus_name": "Rural_Town", "to_bus_name": "Wind_Farm_Connection", "length_km": 10.0, "r_ohm_per_km": 0.2, "x_ohm_per_km": 0.4, "s_nom_mva": 100.0},
]

# Generators (power plants) connected to buses
# - name: Unique identifier for the generator
# - bus_name: Name of the bus where generator is connected
# - p_nom_mw: Nominal active power capacity in MW
# - marginal_cost_eur_per_mwh: Operating cost per MWh
# - control_type: 'Slack' for reference generator, 'PQ' for regular generators
sample_generators = [
    {"name": "Thermal_Plant_A", "bus_name": "Substation_A", "p_nom_mw": 500.0, "marginal_cost_eur_per_mwh": 25.0, "control_type": "Slack"},
    {"name": "Solar_Farm_Rural", "bus_name": "Rural_Town", "p_nom_mw": 80.0, "marginal_cost_eur_per_mwh": 10.0, "control_type": "PQ"},
    {"name": "Wind_Turbines", "bus_name": "Wind_Farm_Connection", "p_nom_mw": 120.0, "marginal_cost_eur_per_mwh": 5.0, "control_type": "PQ"},
]

# Loads (power consumers) connected to buses
# - name: Unique identifier for the load
# - bus_name: Name of the bus where load is connected
# - p_set_mw: Active power consumption in MW
# - q_set_mvar: Reactive power consumption in MVAr
sample_loads = [
    {"name": "City_Load_1", "bus_name": "City_Center", "p_set_mw": 70.0, "q_set_mvar": 20.0},
    {"name": "Industrial_Load_1", "bus_name": "Industrial_Park", "p_set_mw": 100.0, "q_set_mvar": 30.0},
    {"name": "Rural_Load_1", "bus_name": "Rural_Town", "p_set_mw": 30.0, "q_set_mvar": 10.0},
]

# Transformers connecting buses at different voltage levels
# - name: Unique identifier for the transformer
# - from_bus_name, to_bus_name: Names of connected buses
# - s_nom_mva: Nominal apparent power capacity in MVA
# - tap_ratio: Voltage transformation ratio
# - phase_shift: Phase shift angle in degrees
# - vector_group: Transformer vector group (e.g., 'Dyn11')
sample_transformers = [
    {
        "name": "MainSubstation_Transformer",
        "from_bus_name": "Substation_A",
        "to_bus_name": "City_Center",
        "s_nom_mva": 200.0,
        "tap_ratio": 1.0,
        "phase_shift": 0.0,
        "vector_group": "Dyn11"
    },
    {
        "name": "Industrial_Transformer",
        "from_bus_name": "City_Center",
        "to_bus_name": "Industrial_Park",
        "s_nom_mva": 150.0,
        "tap_ratio": 0.95,
        "phase_shift": -30.0,
        "vector_group": "Yyn0"
    }
]

# Storage units for energy storage and grid balancing
# - name: Unique identifier for the storage unit
# - bus_name: Name of the bus where storage is connected
# - p_nom_mw: Nominal power capacity for charging/discharging in MW
# - max_hours: Maximum storage capacity in hours at nominal power
# - efficiency_store: Efficiency of storing energy (0-1)
# - efficiency_dispatch: Efficiency of dispatching energy (0-1)
# - cyclic_soc: Whether state of charge should be cyclic over period
sample_storage_units = [
    {
        "name": "Industrial_Battery",
        "bus_name": "Industrial_Park",
        "p_nom_mw": 50.0,
        "max_hours": 4.0,
        "efficiency_store": 0.95,
        "efficiency_dispatch": 0.95,
        "cyclic_soc": True
    },
    {
        "name": "Grid_Scale_Battery",
        "bus_name": "City_Center",
        "p_nom_mw": 100.0,
        "max_hours": 6.0,
        "efficiency_store": 0.92,
        "efficiency_dispatch": 0.92,
        "cyclic_soc": True
    }
]

# HVDC (High Voltage DC) Links
# - name: Unique identifier for the HVDC link
# - from_bus_name, to_bus_name: Names of connected buses
# - p_nom_mw: Nominal power capacity in MW
# - efficiency: Transmission efficiency (0-1)
# - p_min_pu: Minimum power as per unit of nominal power
# - p_max_pu: Maximum power as per unit of nominal power
sample_hvdc_links = [
    {
        "name": "Offshore_Wind_Link",
        "from_bus_name": "Wind_Farm_Connection",
        "to_bus_name": "City_Center",
        "p_nom_mw": 400.0,
        "efficiency": 0.97,
        "p_min_pu": -1.0,  # Can flow in both directions
        "p_max_pu": 1.0
    },
    {
        "name": "Industrial_DC_Link",
        "from_bus_name": "Substation_A",
        "to_bus_name": "Industrial_Park",
        "p_nom_mw": 300.0,
        "efficiency": 0.98,
        "p_min_pu": 0.0,   # Unidirectional flow
        "p_max_pu": 1.0
    }
]

# Static VAR Compensators (SVCs) for reactive power control
# - name: Unique identifier for the SVC
# - bus_name: Name of the bus where SVC is connected
# - q_nom_mvar: Nominal reactive power capacity in MVAr
# - q_min_mvar: Minimum reactive power output
# - q_max_mvar: Maximum reactive power output
# - v_set_pu: Voltage setpoint in per unit
sample_svcs = [
    {
        "name": "Industrial_SVC",
        "bus_name": "Industrial_Park",
        "q_nom_mvar": 100.0,
        "q_min_mvar": -100.0,
        "q_max_mvar": 100.0,
        "v_set_pu": 1.02
    },
    {
        "name": "Wind_Farm_SVC",
        "bus_name": "Wind_Farm_Connection",
        "q_nom_mvar": 150.0,
        "q_min_mvar": -150.0,
        "q_max_mvar": 150.0,
        "v_set_pu": 1.00
    }
]

def create_connection(db_file):
    """Create a database connection to the SQLite database specified by db_file.
    
    Args:
        db_file: Path to the SQLite database file
        
    Returns:
        Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except sqlite3.Error as e:
        print(e)
    return conn

def create_tables(conn):
    """Create the necessary tables in the database.
    
    Creates four tables:
    - Buses: Power system nodes
    - Lines: Transmission lines connecting buses
    - Generators: Power generation units
    - Loads: Power consumption points
    
    Args:
        conn: Connection object to the database
    """
    c = conn.cursor()
    # Create Buses table
    c.execute('''
        CREATE TABLE IF NOT EXISTS Buses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            v_nom_kv REAL NOT NULL,
            type TEXT NOT NULL CHECK(type IN ('b', 'l')),
            x_coord REAL,
            y_coord REAL
        )
    ''')
    # Create Lines table with foreign key constraints
    c.execute('''
        CREATE TABLE IF NOT EXISTS Lines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            from_bus_id INTEGER NOT NULL,
            to_bus_id INTEGER NOT NULL,
            length_km REAL NOT NULL,
            r_ohm_per_km REAL NOT NULL,
            x_ohm_per_km REAL NOT NULL,
            s_nom_mva REAL NOT NULL,
            FOREIGN KEY (from_bus_id) REFERENCES Buses(id),
            FOREIGN KEY (to_bus_id) REFERENCES Buses(id)
        )
    ''')
    # Create Generators table with foreign key constraint
    c.execute('''
        CREATE TABLE IF NOT EXISTS Generators (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            bus_id INTEGER NOT NULL,
            p_nom_mw REAL NOT NULL,
            marginal_cost_eur_per_mwh REAL NOT NULL,
            control_type TEXT DEFAULT 'PQ' CHECK(control_type IN ('PQ', 'Slack')),
            FOREIGN KEY (bus_id) REFERENCES Buses(id)
        )
    ''')
    # Create Loads table with foreign key constraint
    c.execute('''
        CREATE TABLE IF NOT EXISTS Loads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            bus_id INTEGER NOT NULL,
            p_set_mw REAL NOT NULL,
            q_set_mvar REAL NOT NULL,
            FOREIGN KEY (bus_id) REFERENCES Buses(id)
        )
    ''')
    # Create Transformers table with foreign key constraints
    c.execute('''
        CREATE TABLE IF NOT EXISTS Transformers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            from_bus_id INTEGER NOT NULL,
            to_bus_id INTEGER NOT NULL,
            s_nom_mva REAL NOT NULL,
            tap_ratio REAL NOT NULL,
            phase_shift REAL NOT NULL,
            vector_group TEXT NOT NULL,
            FOREIGN KEY (from_bus_id) REFERENCES Buses(id),
            FOREIGN KEY (to_bus_id) REFERENCES Buses(id)
        )
    ''')
    # Create StorageUnits table with foreign key constraint
    c.execute('''
        CREATE TABLE IF NOT EXISTS StorageUnits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            bus_id INTEGER NOT NULL,
            p_nom_mw REAL NOT NULL,
            max_hours REAL NOT NULL,
            efficiency_store REAL NOT NULL,
            efficiency_dispatch REAL NOT NULL,
            cyclic_soc BOOLEAN NOT NULL,
            FOREIGN KEY (bus_id) REFERENCES Buses(id)
        )
    ''')
    # Create HVDC_Links table with foreign key constraints
    c.execute('''
        CREATE TABLE IF NOT EXISTS HVDC_Links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            from_bus_id INTEGER NOT NULL,
            to_bus_id INTEGER NOT NULL,
            p_nom_mw REAL NOT NULL,
            efficiency REAL NOT NULL,
            p_min_pu REAL NOT NULL,
            p_max_pu REAL NOT NULL,
            FOREIGN KEY (from_bus_id) REFERENCES Buses(id),
            FOREIGN KEY (to_bus_id) REFERENCES Buses(id)
        )
    ''')
    # Create SVCs (Static VAR Compensators) table with foreign key constraint
    c.execute('''
        CREATE TABLE IF NOT EXISTS SVCs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            bus_id INTEGER NOT NULL,
            q_nom_mvar REAL NOT NULL,
            q_min_mvar REAL NOT NULL,
            q_max_mvar REAL NOT NULL,
            v_set_pu REAL NOT NULL,
            FOREIGN KEY (bus_id) REFERENCES Buses(id)
        )
    ''')
    conn.commit()

def insert_data(conn, data_type, data_list, bus_name_to_id=None):
    """Insert data into the specified table.
    
    Args:
        conn: Database connection object
        data_type: Type of data to insert ('Buses', 'Lines', 'Generators', or 'Loads')
        data_list: List of dictionaries containing the data
        bus_name_to_id: Dictionary mapping bus names to their IDs (required for Lines, Generators, and Loads)
    """
    c = conn.cursor()
    if data_type == "Buses":
        for d in data_list:
            c.execute('INSERT OR IGNORE INTO Buses (name, v_nom_kv, type, x_coord, y_coord) VALUES (?, ?, ?, ?, ?)', 
                     (d['name'], d['v_nom_kv'], d['type'], d['x_coord'], d['y_coord']))
    elif data_type == "Lines":
        for d in data_list:
            from_id = bus_name_to_id[d['from_bus_name']]
            to_id = bus_name_to_id[d['to_bus_name']]
            c.execute('INSERT OR IGNORE INTO Lines (name, from_bus_id, to_bus_id, length_km, r_ohm_per_km, x_ohm_per_km, s_nom_mva) VALUES (?, ?, ?, ?, ?, ?, ?)',
                     (d['name'], from_id, to_id, d['length_km'], d['r_ohm_per_km'], d['x_ohm_per_km'], d['s_nom_mva']))
    elif data_type == "Generators":
        for d in data_list:
            bus_id = bus_name_to_id[d['bus_name']]
            c.execute('INSERT OR IGNORE INTO Generators (name, bus_id, p_nom_mw, marginal_cost_eur_per_mwh, control_type) VALUES (?, ?, ?, ?, ?)',
                     (d['name'], bus_id, d['p_nom_mw'], d['marginal_cost_eur_per_mwh'], d['control_type']))
    elif data_type == "Loads":
        for d in data_list:
            bus_id = bus_name_to_id[d['bus_name']]
            c.execute('INSERT OR IGNORE INTO Loads (name, bus_id, p_set_mw, q_set_mvar) VALUES (?, ?, ?, ?)',
                     (d['name'], bus_id, d['p_set_mw'], d['q_set_mvar']))
    elif data_type == "Transformers":
        for d in data_list:
            from_id = bus_name_to_id[d['from_bus_name']]
            to_id = bus_name_to_id[d['to_bus_name']]
            c.execute('INSERT OR IGNORE INTO Transformers (name, from_bus_id, to_bus_id, s_nom_mva, tap_ratio, phase_shift, vector_group) VALUES (?, ?, ?, ?, ?, ?, ?)',
                     (d['name'], from_id, to_id, d['s_nom_mva'], d['tap_ratio'], d['phase_shift'], d['vector_group']))
    elif data_type == "StorageUnits":
        for d in data_list:
            bus_id = bus_name_to_id[d['bus_name']]
            c.execute('INSERT OR IGNORE INTO StorageUnits (name, bus_id, p_nom_mw, max_hours, efficiency_store, efficiency_dispatch, cyclic_soc) VALUES (?, ?, ?, ?, ?, ?, ?)',
                     (d['name'], bus_id, d['p_nom_mw'], d['max_hours'], d['efficiency_store'], d['efficiency_dispatch'], d['cyclic_soc']))
    elif data_type == "HVDC_Links":
        for d in data_list:
            from_id = bus_name_to_id[d['from_bus_name']]
            to_id = bus_name_to_id[d['to_bus_name']]
            c.execute('INSERT OR IGNORE INTO HVDC_Links (name, from_bus_id, to_bus_id, p_nom_mw, efficiency, p_min_pu, p_max_pu) VALUES (?, ?, ?, ?, ?, ?, ?)',
                     (d['name'], from_id, to_id, d['p_nom_mw'], d['efficiency'], d['p_min_pu'], d['p_max_pu']))
    elif data_type == "SVCs":
        for d in data_list:
            bus_id = bus_name_to_id[d['bus_name']]
            c.execute('INSERT OR IGNORE INTO SVCs (name, bus_id, q_nom_mvar, q_min_mvar, q_max_mvar, v_set_pu) VALUES (?, ?, ?, ?, ?, ?)',
                     (d['name'], bus_id, d['q_nom_mvar'], d['q_min_mvar'], d['q_max_mvar'], d['v_set_pu']))
    conn.commit()

def get_bus_name_to_id_mapping(conn):
    """Get a dictionary mapping bus names to their database IDs.
    
    Args:
        conn: Database connection object
        
    Returns:
        Dictionary with bus names as keys and their IDs as values
    """
    c = conn.cursor()
    c.execute('SELECT id, name FROM Buses')
    return {name: id for id, name in c.fetchall()}

def main():
    """Main function to create and populate the power grid database."""
    conn = create_connection(DB_NAME)
    if conn:
        # Create database schema
        create_tables(conn)
        # Insert buses first since other components reference them
        insert_data(conn, "Buses", sample_buses)
        # Get bus ID mapping for foreign key relationships
        bus_name_to_id = get_bus_name_to_id_mapping(conn)
        # Insert other components
        insert_data(conn, "Lines", sample_lines, bus_name_to_id)
        insert_data(conn, "Generators", sample_generators, bus_name_to_id)
        insert_data(conn, "Loads", sample_loads, bus_name_to_id)
        insert_data(conn, "Transformers", sample_transformers, bus_name_to_id)
        insert_data(conn, "StorageUnits", sample_storage_units, bus_name_to_id)
        insert_data(conn, "HVDC_Links", sample_hvdc_links, bus_name_to_id)
        insert_data(conn, "SVCs", sample_svcs, bus_name_to_id)
        conn.close()
        print(f"Database '{DB_NAME}' created and populated successfully!")
    else:
        print("Database connection failed.")

if __name__ == '__main__':
    main()
