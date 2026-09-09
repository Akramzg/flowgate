package upstream

import(
  "bufio"
  "strings"
)

type Event struct{
  ID string
  Type string
  Data string
  Retry int
}

// reads raw SSE text and returns a slice of parsed events.

func ParsedEvents(raw string) []Event{
  var events []Event
  var current Event

  scanner := bufio.NewScanner(strings.NewReader(raw))
  for scanner.Scan(){
    line := scanner.Text()

    switch{
    case line == "":
      if current.Data != ""{
        events = append(events, current)
        current = Event{}
      }

    case strings.HasPrefix(line, "id:"):
      current.ID = strings.TrimSpace(strings.TrimPrefix(line, "id:"))

    case strings.HasPrefix(line, "event:"):
      current.Type = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
    
    case strings.HasPrefix(line, "data:"):
      data := strings.TrimSpace(strings.TrimPrefix(line,"data:"))
      if current.Data == ""{
        current.Data = data
      }else{
        current.Data += "\n" + data //multi line data SSE spec allows it
      }

    case strings.HasPrefix(line, ":"):
      //SSE comment - could be keep alive ping, just discard silently

    case strings.HasPrefix(line, "retry:"):
      //retry interval 
    }
  }
  // flush final event if file didn't end with blank line
  if current.Data != ""{
    events = append(events, current)
  }
  return events
}
