namespace arcturus
#nowarn "40"
module tsb = 

    open System
    open System.IO
    open System.IO.MemoryMappedFiles
    open System.Threading.Tasks


    type BuiltInTypes =
      | WholeNumber
      | Integer
      | Real

    type BuiltInTypeValues =
      | WholeNumber of uint64
      | Integer of int64
      | Real of double

    type Entity = {InsertTime : DateTime; EventTime : DateTime; Value : BuiltInTypeValues}

    type EntityType = {Items : seq<Entity>; t : BuiltInTypes; EventName : string}

    type Partition = {EventItems : seq<EntityType>}

    type Result =
      | Success of EntityType
      | Failure of string

    type EntityMessage = { Entity : Entity; FileOffset : int64; EventName : String}

    type IndexMessage =
      | UpdateIndex of EntityMessage
      | CancelIndexing


   //let OpenOrCreateEntityType(fs : FileStream)(entityType : EntityType) : Result =
   //   fs. 

    let indexerAgent = MailboxProcessor.Start(fun inbox->
        // the message processing function
        let rec messageLoop = async{

            // read a message
            let! msg = inbox.Receive()
            match msg with
            | UpdateIndex(x) ->
              printfn "Updating the index for item %A" x
            | CancelIndexing ->
              printfn "Cancelling indexing"
            // loop to top
            return! messageLoop
            }

        // start the loop
        messageLoop
        )

    let CreateEntity<'T>(eventTime : DateTime)(eventValue : BuiltInTypeValues) : Entity  =
      {InsertTime = DateTime.UtcNow; EventTime = eventTime; Value = eventValue}

    let CreateEntityWholeNumber(eventTime : DateTime)(eventName : string)(eventValue : uint64) : Entity =
      {InsertTime = DateTime.UtcNow; EventTime = eventTime;  Value = WholeNumber(eventValue)}

    let CreateEntityInteger(eventTime : DateTime)(eventValue : int64) : Entity =
      {InsertTime = DateTime.UtcNow; EventTime = eventTime; Value = Integer(eventValue)}

    let CreateEventItemReal(eventTime : DateTime)(eventValue : double) : Entity =
      {InsertTime = DateTime.UtcNow; EventTime = eventTime; Value = Real(eventValue)}

    let AddEntities(entityType: EntityType)(entities : seq<Entity>) : Result =
      Success({entityType with Items = Seq.append entityType.Items entities})

    let WriteEntity(proc : MailboxProcessor<IndexMessage>)(eventName : string)(fs: FileStream)(entity : Entity) : Unit =
      let insertTimeBytes = BitConverter.GetBytes entity.InsertTime.Ticks
      let eventTimeBytes = BitConverter.GetBytes entity.EventTime.Ticks
      let valueBytes = match entity.Value with
                        | WholeNumber(n) -> Array.append [|0uy|] (BitConverter.GetBytes n)
                        | Integer(n) -> Array.append [|1uy|] (BitConverter.GetBytes n)
                        | Real(n) -> Array.append [|2uy|] (BitConverter.GetBytes n)
      let b = insertTimeBytes
                    |> Array.append eventTimeBytes
                    |> Array.append valueBytes
      let bWithLength = (BitConverter.GetBytes b.Length)
                          |> Array.append b
      let currentPos = fs.Position
      let updateIndex : EntityMessage = {Entity = entity; FileOffset = currentPos; EventName = eventName}
      proc.Post (UpdateIndex updateIndex)
      fs.Write(bWithLength, (int) fs.Length, bWithLength.Length)

    let ReadBuffer(data : MemoryMappedViewStream)(numBytesToRead : int) =
      let buffer = Array.zeroCreate numBytesToRead
      // The cast to the int is not very friendly
      let dataRead = data.Read(buffer, (int)data.Position, numBytesToRead)
      (dataRead, buffer)

    let ReadEntity(data: MemoryMappedViewStream) : Entity =
      let (sizeRead, entitySizeBuffer) = ReadBuffer data 8
      let (insertRead, insertTime) = ReadBuffer data 8 
      let (eventRead, eventTime) = ReadBuffer data 8
      let dataType = data.ReadByte()
      let (valueRead, valueBytes) = ReadBuffer data 8
      let value = match dataType with
                   | 0 ->  WholeNumber(BitConverter.ToUInt64(valueBytes, 0)) 
                   | 1 ->  Integer(BitConverter.ToInt64(valueBytes, 0)) 
                   | 2 ->  Real(BitConverter.ToDouble(valueBytes, 0)) 
      {InsertTime = (DateTime.FromBinary (BitConverter.ToInt64(insertTime, 0))); EventTime = (DateTime.FromBinary (BitConverter.ToInt64(eventTime, 0))); Value = value} 
