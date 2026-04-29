import os
import platform

def create_folders():
    print("Processing local workspaces setup")

    if platform.system() == "Windows":
        base_path = r"C:\Aquila_Alpha_Platform"
    else:
        base_path = os.path.join(os.path.expanduser("~"), "Aquila_Alpha_Platform")
    
    paths = [
        base_path,
        os.path.join(base_path, "Workspace_Free"),
        os.path.join(base_path, "Workspace_Advanced"),
        os.path.join(base_path, "Workspace_Advanced", "Portfolio_Risk_Analytics")
    ]
    
    for p in paths:
        os.makedirs(p, exist_ok=True)
        folder_name = os.path.basename(p) if os.path.basename(p) else "Aquila_Alpha_Platform"
        print(f"- {folder_name}: ready")
        
    print("Workspace setup finished")

if __name__ == "__main__":
    create_folders()