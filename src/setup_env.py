import os
import shutil

def create_folders():
    print("Processing local workspaces setup...")

    base_path = os.path.join(os.getcwd(), "Aquila_Alpha_Platform")
    
    workspace_free = os.path.join(base_path, "Workspace_Free")
    workspace_advanced = os.path.join(base_path, "Workspace_Advanced")
    
    paths = [
        base_path,
        workspace_free,
        workspace_advanced,
        os.path.join(workspace_advanced, "Portfolio_Risk_Analytics")
    ]

    for p in paths:
        os.makedirs(p, exist_ok=True)
        folder_name = os.path.basename(p) if os.path.basename(p) else "Aquila_Alpha_Platform"
        print(f"- {folder_name}: ready")

    source_pbix_free = os.path.join("powerbi_dashboards", "Aquila_Dashboard_Free.pbix")
    source_pbix_advanced = os.path.join("powerbi_dashboards", "Aquila_Dashboard_Advanced.pbix")
    
    dest_pbix_free = os.path.join(workspace_free, "Aquila_Dashboard_Free.pbix")
    dest_pbix_advanced = os.path.join(workspace_advanced, "Aquila_Dashboard_Advanced.pbix")

    if os.path.exists(source_pbix_free):
        shutil.copy2(source_pbix_free, dest_pbix_free)
        print(f"Copied Free Dashboard to: Workspace_Free")
    else:
        print(f"Warning: Source file {source_pbix_free} not found. Check the file name.")

    if os.path.exists(source_pbix_advanced):
        shutil.copy2(source_pbix_advanced, dest_pbix_advanced)
        print(f"Copied Advanced Dashboard to: Workspace_Advanced")
    else:
        print(f"Warning: Source file {source_pbix_advanced} not found. Check the file name.")
        
    print("Workspace setup finished!")

if __name__ == "__main__":
    create_folders()