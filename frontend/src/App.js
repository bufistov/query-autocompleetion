import './App.css';
import Autocomplete from "./AutoComplete";
import { autoCompleteData } from "./data.js";

function App() {
  return (
    <div>
      <div className="Header">
        <p>Autocomplete demo</p>
      </div>
      <div className="App">
        <Autocomplete data={autoCompleteData} />
      </div>
    </div>
  );
}

export default App;
