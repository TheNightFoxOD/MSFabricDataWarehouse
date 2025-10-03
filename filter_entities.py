#!/usr/bin/env python3
"""
Script to filter EntityDefinitions.json based on entities_list.txt
"""

import json
import os

def load_entities_list(filepath):
    """Load the list of entities from entities_list.txt"""
    entities = set()
    with open(filepath, 'r', encoding='utf-8') as f:
        for line in f:
            entity = line.strip()
            if entity:  # Skip empty lines
                entities.add(entity)
    return entities

def filter_entity_definitions(input_file, output_file, allowed_entities):
    """Filter EntityDefinitions.json to include only entities in the allowed list"""
    
    # Load the original JSON file
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Filter the entities based on LogicalName
    filtered_entities = []
    
    for entity in data['value']:
        logical_name = entity.get('LogicalName', '')
        if logical_name in allowed_entities:
            filtered_entities.append(entity)
    
    # Create the filtered data structure
    filtered_data = {
        "@odata.context": data["@odata.context"],
        "value": filtered_entities
    }
    
    # Write the filtered data to the output file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(filtered_data, f, indent=4, ensure_ascii=False)
    
    return len(filtered_entities)

def main():
    # File paths
    docs_dir = r"f:\Work\OpenDoors\MSFabricDataWarehouse\Docs"
    entities_list_file = os.path.join(docs_dir, "entities_list.txt")
    input_file = os.path.join(docs_dir, "EntityDefinitions.json")
    output_file = os.path.join(docs_dir, "SelectedEntityDefinitions.json")
    
    try:
        # Load the list of allowed entities
        print("Loading entities list...")
        allowed_entities = load_entities_list(entities_list_file)
        print(f"Found {len(allowed_entities)} entities in the list")
        
        # Filter the entity definitions
        print("Filtering entity definitions...")
        filtered_count = filter_entity_definitions(input_file, output_file, allowed_entities)
        
        print(f"Successfully created {output_file}")
        print(f"Filtered from original entities to {filtered_count} matching entities")
        
        # Show some statistics
        with open(input_file, 'r', encoding='utf-8') as f:
            original_data = json.load(f)
        original_count = len(original_data['value'])
        
        print(f"Original file had {original_count} entities")
        print(f"Filtered file has {filtered_count} entities")
        print(f"Reduction: {original_count - filtered_count} entities removed")
        
    except FileNotFoundError as e:
        print(f"Error: File not found - {e}")
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON format - {e}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()