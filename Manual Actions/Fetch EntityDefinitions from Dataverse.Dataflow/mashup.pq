[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "EntityDefinitions_DataDestination", IsNewTarget = false], Settings = [Kind = "Manual", AllowCreation = false, ColumnSettings = [Mappings = {[SourceColumnName = "LogicalName", DestinationColumnName = "LogicalName"], [SourceColumnName = "PrimaryIdAttribute", DestinationColumnName = "PrimaryIdAttribute"]}], DynamicSchema = false, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared EntityDefinitions = let
  Source = OData.Feed("https://od-master.crm4.dynamics.com/api/data/v9.2/EntityDefinitions?$select=LogicalName,PrimaryIdAttribute", null, [Implementation = "2.0"])
in
  Source;
shared EntityDefinitions_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "b0f83c07-a701-49bb-a165-e06ca0ee4000"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "4aee8a32-be91-489f-89f3-1a819b188807"]}[Data],
  TableNavigation = Navigation_2{[Id = "entity_definitions", ItemKind = "Table"]}[Data]
in
  TableNavigation;
