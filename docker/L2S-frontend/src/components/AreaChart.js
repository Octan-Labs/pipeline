import ApexCharts from "apexcharts";
import React from "react";
import ReactApexChart from "react-apexcharts";
import "../index.css";
import { formatTotalVolume } from "../utils";
import DUMMY_DATA from "./dummy-data-chart";

// const ApexChart = () => {
//   const [data, setData] = useState([]);
//   const [selection, setSelection] = useState("one_year");
//   const [timeline, setTimeline] = useState("31 Aug 2023 - 14 Oct 2023");

//   useEffect(() => {
//     const getDataTvl = async () => {
//       try {
//         const { data } = await fetch("http://54.255.150.198/api/tvl", {
//           method: "GET",
//           headers: {
//             "Content-Type": "application/json",
//           },
//         }).then((res) => res.json());
//         const result = data.map(Object.values);
//         setData(result);
//       } catch (e) {
//         console.log(e);
//       }
//     };

//     getDataTvl();
//   }, []);

//   const option = {
//     series: [
//       {
//         data: data.length > 0 ? data : DUMMY_DATA.data,
//       },
//     ],
//     options: {
//       chart: {
//         id: "area-datetime",
//         type: "area",
//         height: 350,
//         zoom: {
//           autoScaleYaxis: true,
//         },
//       },
//       dataLabels: {
//         enabled: false,
//       },
//       markers: {
//         size: 0,
//         style: "hollow",
//       },
//       xaxis: {
//         type: "datetime",
//         min: new Date("31 Aug 2023").getTime(),
//         tickAmount: 6,
//       },
//       tooltip: {
//         x: {
//           format: "dd MMM yyyy",
//         },
//         custom: function ({ series, seriesIndex, dataPointIndex }) {
//           return `
//                     <div>
//                         <span>USD: ${formatTotalVolume(
//                           series[seriesIndex][dataPointIndex]
//                         )}$</span>
//                     </div>
//                     `;
//         },
//       },
//       fill: {
//         type: "gradient",
//         gradient: {
//           shadeIntensity: 1,
//           opacityFrom: 0.7,
//           opacityTo: 0.9,
//           stops: [0, 100],
//         },
//       },
//     },

//     selection: selection,

//     timeline: timeline,
//   };

//   const updateData = (timeline) => {
//     setSelection(timeline);
//     switch (timeline) {
//       case "one_month":
//         ApexCharts.exec(
//           "area-datetime",
//           "zoomX",
//           new Date("08 Oct 2023").getTime(),
//           new Date("12 Oct 2023").getTime()
//         );
//         setTimeline("2023 Oct 08, Oct 14");
//         break;
//       case "six_months":
//         ApexCharts.exec(
//           "area-datetime",
//           "zoomX",
//           new Date("08 Oct 2023").getTime(),
//           new Date("12 Oct 2023").getTime()
//         );
//         setTimeline("2023 Oct 08, Oct 14");
//         break;
//       case "one_year":
//         ApexCharts.exec(
//           "area-datetime",
//           "zoomX",
//           new Date("08 Oct 2023").getTime(),
//           new Date("12 Oct 2023").getTime()
//         );
//         setTimeline("2023 Oct 08, Oct 14");
//         break;
//       case "ytd":
//         ApexCharts.exec(
//           "area-datetime",
//           "zoomX",
//           new Date("08 Oct 2023").getTime(),
//           new Date("12 Oct 2023").getTime()
//         );
//         setTimeline("2023 Oct 08, Oct 14");
//         break;
//       case "all":
//         ApexCharts.exec(
//           "area-datetime",
//           "zoomX",
//           new Date("08 Oct 2023").getTime(),
//           new Date("12 Oct 2023").getTime()
//         );
//         setTimeline("2023 Oct 08, Oct 14");
//         break;
//       default:
//     }
//   };

//   return (
//     <div className="flex flex-col gap-4 mt-4" id="chart">
//       <div className="flex justify-between">
//         <p className="flex h-8 max-w-[130px] items-center font-bold sm:max-w-full">
//           {timeline}
//         </p>
//         <div className="toolbar">
//           <button
//             id="one_month"
//             onClick={() => updateData("one_month")}
//             className={`p-2 ${selection === "one_month" && "bg-slate-200"}`}
//           >
//             1M
//           </button>
//           &nbsp;
//           <button
//             id="six_months"
//             onClick={() => updateData("six_months")}
//             className={`p-2 ${selection === "six_months" && "bg-slate-200"}`}
//           >
//             6M
//           </button>
//           &nbsp;
//           <button
//             id="one_year"
//             onClick={() => updateData("one_year")}
//             className={`p-2 ${selection === "one_year" && "bg-slate-200"}`}
//           >
//             1Y
//           </button>
//           &nbsp;
//           <button
//             id="ytd"
//             onClick={() => updateData("ytd")}
//             className={`p-2 ${selection === "ytd" && "bg-slate-200"}`}
//           >
//             YTD
//           </button>
//           &nbsp;
//           <button
//             id="all"
//             onClick={() => updateData("all")}
//             className={`p-2 ${selection === "all" && "bg-slate-200"}`}
//           >
//             ALL
//           </button>
//         </div>
//       </div>

//       <div id="chart-timeline">
//         <ReactApexChart
//           options={option}
//           series={option.series}
//           type="area"
//           height={350}
//         />
//       </div>
//     </div>
//   );
// };
class ApexChart extends React.Component {
  constructor(props) {
    super(props);
    let tvlArray = props.data.map(Object.values);
    console.log(tvlArray);
    this.state = {
      series: [
        {
          data: tvlArray.length > 0 ? tvlArray : DUMMY_DATA.data,
          // data: DUMMY_DATA.data,
        },
      ],
      options: {
        chart: {
          id: "area-datetime",
          type: "area",
          height: 350,
          zoom: {
            autoScaleYaxis: true,
          },
        },
        dataLabels: {
          enabled: false,
        },
        markers: {
          size: 0,
          style: "hollow",
        },
        xaxis: {
          type: "datetime",
          min: new Date("31 Aug 2023").getTime(),
          tickAmount: 6,
        },
        tooltip: {
          x: {
            format: "dd MMM yyyy",
          },
          custom: function ({ series, seriesIndex, dataPointIndex }) {
            return `
                <div>
                    <span>USD: ${formatTotalVolume(
                      series[seriesIndex][dataPointIndex]
                    )}$</span>
                </div>
                `;
          },
        },
        fill: {
          type: "gradient",
          gradient: {
            shadeIntensity: 1,
            opacityFrom: 0.7,
            opacityTo: 0.9,
            stops: [0, 100],
          },
        },
      },

      selection: "one_year",

      timeline: "08 Oct 2023 - 14 Oct 2023",
    };
  }

  async componentDidMount() {
    const { data } = await fetch("https://l2s-api.octan.network/api/tvl").then(
      (response) => response.json()
    );
    let tvlArray = data.map(Object.values);
    this.setState({
      series: [
        {
          data: tvlArray,
        },
      ],
    });
  }

  updateData(timeline) {
    this.setState({
      selection: timeline,
    });

    switch (timeline) {
      case "one_month":
        ApexCharts.exec(
          "area-datetime",
          "zoomX",
          new Date("08 Oct 2023").getTime(),
          new Date("12 Oct 2023").getTime()
        );
        this.setState({
          timeline: "2023 Oct 08, Oct 12",
        });
        break;
      case "six_months":
        ApexCharts.exec(
          "area-datetime",
          "zoomX",
          new Date("08 Oct 2023").getTime(),
          new Date("12 Oct 2023").getTime()
        );
        this.setState({
          timeline: "2023 Oct 08, Oct 12",
        });
        break;
      case "one_year":
        ApexCharts.exec(
          "area-datetime",
          "zoomX",
          new Date("08 Oct 2023").getTime(),
          new Date("12 Oct 2023").getTime()
        );
        this.setState({
          timeline: "2023 Oct 08, Oct 12",
        });
        break;
      case "ytd":
        ApexCharts.exec(
          "area-datetime",
          "zoomX",
          new Date("08 Oct 2023").getTime(),
          new Date("12 Oct 2023").getTime()
        );
        this.setState({
          timeline: "2023 Oct 08, Oct 12",
        });
        break;
      case "all":
        ApexCharts.exec(
          "area-datetime",
          "zoomX",
          new Date("08 Oct 2023").getTime(),
          new Date("12 Oct 2023").getTime()
        );
        this.setState({
          timeline: "2023 Oct 08, Oct 12",
        });
        break;
      default:
    }
  }

  render() {
    return (
      <div className="flex flex-col gap-4 mt-4" id="chart">
        <div className="flex justify-between">
          <p className="flex h-8 max-w-[130px] items-center font-bold sm:max-w-full">
            {this.state.timeline}
          </p>
          <div className="toolbar">
            <button
              id="one_month"
              onClick={() => this.updateData("one_month")}
              className={`p-2 ${
                this.state.selection === "one_month" && "bg-slate-200"
              }`}
            >
              1M
            </button>
            &nbsp;
            <button
              id="six_months"
              onClick={() => this.updateData("six_months")}
              className={`p-2 ${
                this.state.selection === "six_months" && "bg-slate-200"
              }`}
            >
              6M
            </button>
            &nbsp;
            <button
              id="one_year"
              onClick={() => this.updateData("one_year")}
              className={`p-2 ${
                this.state.selection === "one_year" && "bg-slate-200"
              }`}
            >
              1Y
            </button>
            &nbsp;
            <button
              id="ytd"
              onClick={() => this.updateData("ytd")}
              className={`p-2 ${
                this.state.selection === "ytd" && "bg-slate-200"
              }`}
            >
              YTD
            </button>
            &nbsp;
            <button
              id="all"
              onClick={() => this.updateData("all")}
              className={`p-2 ${
                this.state.selection === "all" && "bg-slate-200"
              }`}
            >
              ALL
            </button>
          </div>
        </div>

        <div id="chart-timeline">
          <ReactApexChart
            options={this.state.options}
            series={this.state.series}
            type="area"
            height={350}
          />
        </div>
      </div>
    );
  }
}

export default ApexChart;
