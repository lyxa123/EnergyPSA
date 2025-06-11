# Power System Analysis using PyPSA
# This script loads power system data from SQLite database into a PyPSA network
# and performs power flow optimization and visualization

import pypsa
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt

# Database configuration
DB_NAME = 'power_grid.db'

def create_connection(db_file):
    """Create a database connection to the SQLite database.
    
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

def load_network_from_db(conn):
    """Create a PyPSA network and load components from the database.
    
    This function:
    1. Creates an empty PyPSA network
    2. Sets up a single snapshot (time point) for analysis
    3. Loads all power system components from the database
    4. Configures the network components with their parameters
    
    Args:
        conn: Database connection object
        
    Returns:
        PyPSA Network object populated with power system components
    """
    # Initialize empty network with a single snapshot
    n = pypsa.Network()
    n.set_snapshots(pd.Index(['2025-01-01 00:00:00']))
    c = conn.cursor()

    # Load buses (nodes) from database
    c.execute('SELECT id, name, v_nom_kv, type, x_coord, y_coord FROM Buses')
    buses = c.fetchall()
    bus_id_to_name = {}
    for bus_id, name, v_nom, bus_type, x, y in buses:
        n.add("Bus", name, v_nom=v_nom, type=bus_type)
        n.buses.loc[name, 'x'] = x
        n.buses.loc[name, 'y'] = y
        bus_id_to_name[bus_id] = name

    # Load transmission lines
    c.execute('SELECT name, from_bus_id, to_bus_id, length_km, r_ohm_per_km, x_ohm_per_km, s_nom_mva FROM Lines')
    for name, from_id, to_id, length, r_ohm, x_ohm, s_nom in c.fetchall():
        n.add(
            "Line", name,
            bus0=bus_id_to_name[from_id],
            bus1=bus_id_to_name[to_id],
            length=length,
            r=r_ohm * length,
            x=x_ohm * length,
            s_nom=s_nom
        )

    # Load transformers
    c.execute('SELECT name, from_bus_id, to_bus_id, s_nom_mva, tap_ratio, phase_shift, vector_group FROM Transformers')
    for name, from_id, to_id, s_nom, tap, phase, vector in c.fetchall():
        n.add(
            "Transformer", name,
            bus0=bus_id_to_name[from_id],
            bus1=bus_id_to_name[to_id],
            s_nom=s_nom,
            tap_ratio=tap,
            phase_shift=phase,
            model="t",  # Use the 'ideal' transformer model for better convergence
            x=0.1,     # Add some reactance for numerical stability
            r=0.01     # Add minimal resistance
        )

    # Load generators
    c.execute('SELECT name, bus_id, p_nom_mw, marginal_cost_eur_per_mwh, control_type FROM Generators')
    for name, bus_id, p_nom, cost, ctrl in c.fetchall():
        n.add(
            "Generator", name,
            bus=bus_id_to_name[bus_id],
            p_nom=p_nom,
            marginal_cost=cost,
            control=ctrl
        )

    # Load power consumers
    c.execute('SELECT name, bus_id, p_set_mw, q_set_mvar FROM Loads')
    for name, bus_id, p_set, q_set in c.fetchall():
        n.add(
            "Load", name,
            bus=bus_id_to_name[bus_id],
            p_set=p_set,
            q_set=q_set
        )

    # Load storage units
    c.execute('SELECT name, bus_id, p_nom_mw, max_hours, efficiency_store, efficiency_dispatch, cyclic_soc FROM StorageUnits')
    for name, bus_id, p_nom, max_hours, eff_store, eff_dispatch, cyclic in c.fetchall():
        n.add(
            "StorageUnit", name,
            bus=bus_id_to_name[bus_id],
            p_nom=p_nom,
            max_hours=max_hours,
            efficiency_store=eff_store,
            efficiency_dispatch=eff_dispatch,
            cyclic_state_of_charge=cyclic
        )

    # Load HVDC links
    c.execute('SELECT name, from_bus_id, to_bus_id, p_nom_mw, efficiency, p_min_pu, p_max_pu FROM HVDC_Links')
    for name, from_id, to_id, p_nom, efficiency, p_min, p_max in c.fetchall():
        n.add(
            "Link", name,
            bus0=bus_id_to_name[from_id],
            bus1=bus_id_to_name[to_id],
            p_nom=p_nom,
            efficiency=efficiency,
            p_min_pu=p_min,
            p_max_pu=p_max
        )

    # Load SVCs and implement them as reactive power generators
    c.execute('SELECT name, bus_id, q_nom_mvar, q_min_mvar, q_max_mvar, v_set_pu FROM SVCs')
    for name, bus_id, q_nom, q_min, q_max, v_set in c.fetchall():
        # Add SVC as a generator with reactive power control
        n.add(
            "Generator", f"SVC_{name}",
            bus=bus_id_to_name[bus_id],
            p_nom=0,  # No active power
            q_nom=q_nom,  # Reactive power rating
            q_min=q_min,  # Min reactive power
            q_max=q_max,  # Max reactive power
            control="PV",  # Voltage control mode
            v_set=v_set   # Voltage setpoint
        )

    return n

def validate_network_parameters(n):
    """Validate network parameters and provide diagnostic feedback.
    
    This function checks if component parameters are within typical ranges
    and provides warnings/explanations for unusual values.
    
    Args:
        n: PyPSA Network object
    
    Returns:
        list: List of (component, message) tuples for any detected issues
    """
    issues = []
    
    # Validate transformers
    for transformer in n.transformers.index:
        # Check impedance values
        x = n.transformers.loc[transformer, 'x']
        r = n.transformers.loc[transformer, 'r']
        if x < 0.01 or x > 0.2:
            issues.append((
                f"Transformer {transformer}",
                f"Unusual reactance value {x:.3f} p.u. (typical range: 0.01-0.2 p.u.). " +
                "This may cause convergence issues or unrealistic power flows."
            ))
        if r > 0.1:
            issues.append((
                f"Transformer {transformer}",
                f"High resistance value {r:.3f} p.u. (should be < 0.1 p.u.). " +
                "This may cause excessive losses in the simulation."
            ))
        
        # Check tap ratio
        tap = n.transformers.loc[transformer, 'tap_ratio']
        if tap < 0.9 or tap > 1.1:
            issues.append((
                f"Transformer {transformer}",
                f"Unusual tap ratio {tap:.2f} (typical range: 0.9-1.1). " +
                "This may cause extreme voltage transformations."
            ))
        
        # Check phase shift
        phase = n.transformers.loc[transformer, 'phase_shift']
        if abs(phase) > 60 or phase % 30 != 0:
            issues.append((
                f"Transformer {transformer}",
                f"Unusual phase shift {phase}° (typically multiples of 30° up to ±60°). " +
                "This may cause unexpected power flow patterns."
            ))
    
    # Validate SVCs (implemented as generators)
    svc_gens = [g for g in n.generators.index if g.startswith('SVC_')]
    for svc in svc_gens:
        # Check reactive power limits
        q_nom = n.generators.loc[svc, 'q_nom']
        q_min = n.generators.loc[svc, 'q_min']
        q_max = n.generators.loc[svc, 'q_max']
        
        if abs(q_max - q_min) > 2 * abs(q_nom):
            issues.append((
                f"SVC {svc}",
                f"Reactive power range ({q_min:.1f} to {q_max:.1f} MVAr) " +
                f"is more than twice the nominal capacity ({q_nom:.1f} MVAr). " +
                "This may cause control instability."
            ))
        
        # Check voltage setpoint
        v_set = n.generators.loc[svc, 'v_set']
        if v_set < 0.95 or v_set > 1.05:
            issues.append((
                f"SVC {svc}",
                f"Unusual voltage setpoint {v_set:.2f} p.u. (typical range: 0.95-1.05 p.u.). " +
                "This may cause voltage regulation problems."
            ))
    
    # Validate buses
    for bus in n.buses.index:
        v_nom = n.buses.loc[bus, 'v_nom']
        if v_nom not in [11, 33, 66, 110, 132, 220, 275, 400]:  # Common voltage levels
            issues.append((
                f"Bus {bus}",
                f"Unusual nominal voltage {v_nom:.1f} kV. " +
                "Consider using standard voltage levels for better compatibility."
            ))
    
    # Validate lines
    for line in n.lines.index:
        r = n.lines.loc[line, 'r']
        x = n.lines.loc[line, 'x']
        length = n.lines.loc[line, 'length']
        
        # Check R/X ratio
        if r > 0 and x > 0:  # Avoid division by zero
            rx_ratio = r/x
            if rx_ratio > 2 or rx_ratio < 0.1:
                issues.append((
                    f"Line {line}",
                    f"Unusual R/X ratio {rx_ratio:.2f} (typical range: 0.1-2). " +
                    "This may cause unrealistic power flows."
                ))
        
        # Check impedance per km
        if length > 0:  # Avoid division by zero
            z_per_km = ((r**2 + x**2)**0.5) / length
            if z_per_km > 1.0:  # Unusually high impedance
                issues.append((
                    f"Line {line}",
                    f"High impedance per km: {z_per_km:.2f} ohm/km. " +
                    "This may cause excessive voltage drops."
                ))
    
    return issues

def main():
    """Main function to load network from database and perform analysis."""
    conn = create_connection(DB_NAME)
    if conn:
        # Load power system data into PyPSA network
        n = load_network_from_db(conn)
        conn.close()
        print("Loaded network from database!")

        try:
            # Validate network parameters before analysis
            issues = validate_network_parameters(n)
            if issues:
                print("\nPotential issues detected in network parameters:")
                for component, message in issues:
                    print(f"\n{component}:")
                    print(f"  {message}")
                user_input = input("\nContinue with analysis despite warnings? (y/n): ")
                if user_input.lower() != 'y':
                    print("Analysis cancelled.")
                    return

            # Set initial conditions for better convergence
            for bus_name in n.buses.index:
                n.buses.loc[bus_name, "v_mag_pu_set"] = 1.0  # Set initial voltage to 1.0 p.u.
                
            # Set slack bus parameters
            slack_gen = n.generators[n.generators.control == "Slack"].index[0]
            n.generators.loc[slack_gen, "v_mag_pu_set"] = 1.02  # Slightly higher voltage at slack bus
            n.generators.loc[slack_gen, "p_set"] = 200.0  # Initial power output estimate
            
            # Set initial power for PQ generators
            pq_gens = n.generators[n.generators.control == "PQ"].index
            for gen in pq_gens:
                if "Solar" in gen:
                    n.generators.loc[gen, "p_set"] = 40.0  # 50% of capacity
                elif "Wind" in gen:
                    n.generators.loc[gen, "p_set"] = 60.0  # 50% of capacity
            
            # Set voltage setpoints for PV buses (SVCs)
            svc_gens = [g for g in n.generators.index if g.startswith('SVC_')]
            for gen in svc_gens:
                n.generators.loc[gen, "control"] = "PQ"  # Change to PQ for better convergence
                n.generators.loc[gen, "q_set"] = 0.0  # Start with neutral reactive power
            
            # Initialize storage units
            for storage in n.storage_units.index:
                n.storage_units.loc[storage, "p_set"] = 0.0  # Start with no charging/discharging
            
            # Initialize HVDC links
            for link in n.links.index:
                n.links.loc[link, "p_set"] = 0.0  # Start with no power flow
            
            # Perform power flow analysis
            n.pf()  # Run power flow with default parameters
            print("Power flow analysis successful!")
            
            # Print analysis results
            print("\nResults of Power System Analysis:")
            
            print("\nBus Voltage Magnitudes (p.u.):")
            print(n.buses_t.v_mag_pu.iloc[0])
            
            print("\nGenerator Active Power (MW):")
            print(n.generators_t.p.iloc[0])
            
            print("\nLine Power Flow (MW):")
            print(n.lines_t.p0.iloc[0])
            
            print("\nTransformer Power Flow (MW):")
            print(n.transformers_t.p0.iloc[0])
            
            print("\nStorage Units State:")
            print("Active Power (MW):")
            print(n.storage_units_t.p.iloc[0])
            print("State of Charge (MWh):")
            print(n.storage_units_t.state_of_charge.iloc[0])
            
            print("\nHVDC Links Power Flow (MW):")
            print(n.links_t.p0.iloc[0])
            
            print("\nSVC Reactive Power (MVAr):")
            svc_gens = [g for g in n.generators.index if g.startswith('SVC_')]
            print(n.generators_t.q.loc['2025-01-01 00:00:00', svc_gens])

            # Create network visualization
            fig, ax = plt.subplots(figsize=(15, 10))
            
            # Plot basic network topology
            n.plot.map(ax=ax, bus_sizes=0.1, line_widths=1.0)
            
            # Add bus labels with better positioning
            for _, bus in n.buses.iterrows():
                ax.text(bus.x, bus.y + 0.5, 
                       f"{bus.name}\n({bus.v_nom:.0f}kV)", 
                       fontsize=10, 
                       ha='center', 
                       va='bottom',
                       bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', pad=2))
            
            # Add generator markers and labels
            for _, gen in n.generators[~n.generators.index.str.startswith('SVC_')].iterrows():
                bus = n.buses.loc[gen.bus]
                ax.plot(bus.x, bus.y - 0.3, 's', color='green', markersize=10)
                p_val = n.generators_t.p.loc['2025-01-01 00:00:00', gen.name]
                ax.text(bus.x, bus.y - 0.8, 
                       f"Gen: {gen.name}\n({p_val:.1f}MW)", 
                       fontsize=9, 
                       color='darkgreen',
                       ha='center',
                       va='top',
                       bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', pad=2))
            
            # Add load markers and labels
            for _, load in n.loads.iterrows():
                bus = n.buses.loc[load.bus]
                ax.plot(bus.x + 0.3, bus.y, 'o', color='red', markersize=8)
                ax.text(bus.x + 0.5, bus.y, 
                       f"Load: {load.name}\n({load.p_set:.1f}MW)", 
                       fontsize=9, 
                       color='darkred',
                       ha='left',
                       va='center',
                       bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', pad=2))
            
            # Add storage unit markers and labels
            for _, storage in n.storage_units.iterrows():
                bus = n.buses.loc[storage.bus]
                ax.plot(bus.x - 0.3, bus.y, 'h', color='blue', markersize=8)
                p_val = n.storage_units_t.p.loc['2025-01-01 00:00:00', storage.name]
                ax.text(bus.x - 0.5, bus.y, 
                       f"Storage: {storage.name}\n({p_val:.1f}MW)", 
                       fontsize=9, 
                       color='darkblue',
                       ha='right',
                       va='center',
                       bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', pad=2))
            
            # Add SVC markers and labels
            for gen_name in svc_gens:
                gen = n.generators.loc[gen_name]
                bus = n.buses.loc[gen.bus]
                ax.plot(bus.x, bus.y + 0.3, '^', color='purple', markersize=8)
                q_val = n.generators_t.q.loc['2025-01-01 00:00:00', gen_name]
                ax.text(bus.x, bus.y + 0.8, 
                       f"SVC: {gen_name[4:]}\n({q_val:.1f}MVAr)", 
                       fontsize=9, 
                       color='purple',
                       ha='center',
                       va='bottom',
                       bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', pad=2))
            
            ax.set_title("PyPSA Network Power Flow Analysis", pad=20, fontsize=14)
            plt.grid(True, alpha=0.3)
            
            # Add legend
            legend_elements = [
                plt.Line2D([0], [0], marker='s', color='w', markerfacecolor='green', markersize=10, label='Generator'),
                plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='red', markersize=10, label='Load'),
                plt.Line2D([0], [0], marker='h', color='w', markerfacecolor='blue', markersize=10, label='Storage'),
                plt.Line2D([0], [0], marker='^', color='w', markerfacecolor='purple', markersize=10, label='SVC')
            ]
            ax.legend(handles=legend_elements, loc='upper right')
            
            # Adjust plot margins and layout
            plt.margins(x=0.2, y=0.2)
            plt.tight_layout()
            
            plt.show()

        except Exception as e:
            print(f"Analysis Error: {e}")
            raise  # Re-raise the exception for debugging
    else:
        print("Database connection failed.")

if __name__ == '__main__':
    main()
