import './App.css';
import Autocomplete from "./AutoComplete";
import { autoCompleteData } from "./data.js";

function App() {
  return (
    <div className="container1">
      <div className="Header">
        <h2>Query autocomplete demo</h2>
      </div>
      <div className="App">
        <Autocomplete data={autoCompleteData} />
      </div>
    </div>
  );
}

export default App;
