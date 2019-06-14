package packet

const (
	ProtocolRequest byte = iota
	ProtocolResponse
	ServerDisconnect
	ConnectSuccess
	ConnectFailure
	HandshakeChallenge
	ChatReceive
	UniverseTimeUpdate
	CelestialResponse
	WarpResult
	PlanetType
	Pause
	ClientDisconnectRequest
	ClientConnect
	HandshakeResponse
	PlayerWarp
	FlyShip
	ChatSend
	CelestialRequest
	ClientContextUpdate
	WorldStart
	WorldStop
	WorldLayoutUpdate
	WorldParametersUpdate
	CentralStructureUpdate
	TileArrayUpdate
	TileUpdate
	TileLiquidUpdate
	TileDamageUpdate
	TileModificationFailure
	GiveItem
	EnvironmentUpdate
	UpdateTileProtection
	SetDungeonGravity
	SetDungeonBreathable
	SetPlayerStart
	FindUniqueEntityResponse
	ModifyTileList
	DamageTileGroup
	CollectLiquid
	RequestDrop
	SpawnEntity
	ConnectWire
	DisconnectAllWires
	WolrdClientStateUpdate
	FindUniqueEntity
	WorldStartAcknowledge
	EntityCreate
	EntityUpdateSet
	EntityDestroy
	EntityInteract
	EntityInteractResult
	HitResult
	DamageRequest
	DamageNotification
	EntityMessage
	EntityMessageResponse
	UpdateWorldProperties
	StepUpdate
	SystemWorldStart
	SystemWorldUpdate
	SystemObjectCreate
	SystemObjectDestroy
	SystemShipCreate
	SystemShipDestroy
	SystemObjectSpawn
)

var (
	pktTypes map[byte]string = map[byte]string{
		ProtocolRequest:          "ProtocolRequest",
		ProtocolResponse:         "ProtocolResponse",
		ServerDisconnect:         "ServerDisconnect",
		ConnectSuccess:           "ConnectSuccess",
		ConnectFailure:           "ConnectFailure",
		HandshakeChallenge:       "HandshakeChallenge",
		ChatReceive:              "ChatReceive",
		UniverseTimeUpdate:       "UniverseTimeUpdate",
		CelestialResponse:        "CelestialResponse",
		WarpResult:               "WarpResult",
		PlanetType:               "PlanetType",
		Pause:                    "Pause",
		ClientConnect:            "ClientConnect",
		ClientDisconnectRequest:  "ClientDisconnectRequest",
		HandshakeResponse:        "HandshakeResponse",
		PlayerWarp:               "PlayerWarp",
		FlyShip:                  "FlyShip",
		ChatSend:                 "ChatSend",
		CelestialRequest:         "CelestialRequest",
		ClientContextUpdate:      "ClientContextUpdate",
		WorldStart:               "WorldStart",
		WorldStop:                "WorldStop",
		WorldLayoutUpdate:        "WorldLayoutUpdate",
		WorldParametersUpdate:    "WorldParametersUpdate",
		CentralStructureUpdate:   "CentralStructureUpdate",
		TileArrayUpdate:          "TileArrayUpdate",
		TileUpdate:               "TileUpdate",
		TileLiquidUpdate:         "TileLiquidUpdate",
		TileDamageUpdate:         "TileDamageUpdate",
		TileModificationFailure:  "TileModificationFailure",
		GiveItem:                 "GiveItem",
		EnvironmentUpdate:        "EnvironmentUpdate",
		UpdateTileProtection:     "UpdateTileProtection",
		SetDungeonGravity:        "SetDungeonGravity",
		SetDungeonBreathable:     "SetDungeonBreathable",
		SetPlayerStart:           "SetPlayerStart",
		FindUniqueEntityResponse: "FindUniqueEntityResponse",
		ModifyTileList:           "ModifyTileList",
		DamageTileGroup:          "DamageTileGroup",
		CollectLiquid:            "CollectLiquid",
		RequestDrop:              "RequestDrop",
		SpawnEntity:              "SpawnEntity",
		ConnectWire:              "ConnectWire",
		DisconnectAllWires:       "DisconnectAllWires",
		WolrdClientStateUpdate:   "WolrdClientStateUpdate",
		FindUniqueEntity:         "FindUniqueEntity",
		WorldStartAcknowledge:    "WorldStartAcknowledge",
		EntityCreate:             "EntityCreate",
		EntityUpdateSet:          "EntityUpdateSet",
		EntityDestroy:            "EntityDestroy",
		EntityInteract:           "EntityInteract",
		EntityInteractResult:     "EntityInteractResult",
		HitResult:                "HitResult",
		DamageRequest:            "DamageRequest",
		DamageNotification:       "DamageNotification",
		EntityMessage:            "EntityMessage",
		EntityMessageResponse:    "EntityMessageResponse",
		UpdateWorldProperties:    "UpdateWorldProperties",
		StepUpdate:               "StepUpdate",
		SystemWorldStart:         "SystemWorldStart",
		SystemWorldUpdate:        "SystemWorldUpdate",
		SystemObjectCreate:       "SystemObjectCreate",
		SystemObjectDestroy:      "SystemObjectDestroy",
		SystemShipCreate:         "SystemShipCreate",
		SystemShipDestroy:        "SystemShipDestroy",
		SystemObjectSpawn:        "SystemObjectSpawn",
	}
)
