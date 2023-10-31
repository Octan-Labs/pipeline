import { createBrowserRouter } from "react-router-dom";
import { Summary } from "../pages/summary";

export const router = createBrowserRouter([
  {
    path: "/",
    element: <Summary />,
  },
]);
