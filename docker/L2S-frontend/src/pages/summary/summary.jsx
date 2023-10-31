import { useState, useEffect } from "react";
import ApexChart from "../../components/AreaChart";
import DataTable from "../../components/DataTable";
import { comparePerTvlLock, formatTotalVolume } from "../../utils";
import "../../index.css";

export const Summary = () => {
  const [tvlData, setTvlData] = useState([]);

  useEffect(() => {
    const getDataTvl = async () => {
      try {
        const { data } = await fetch("https://l2s-api.octan.network/api/tvl", {
          method: "GET",
          headers: {
            "Content-Type": "application/json",
          },
        }).then((res) => res.json());
        setTvlData(data);
      } catch (e) {
        console.log(e);
      }
    };

    getDataTvl();
  }, []);

  return (
    <div className="flex flex-col w-full mx-auto max-w-[1120px]">
      <div className="flex flex-col mt-4 justify-between text-base md:mt-12 md:flex-row">
        <div className="">
          <h1 className="mb-1 text-3xl font-bold">Total Value Locked</h1>
          <p className="hidden text-gray-500 dark:text-gray-600 md:block">
            USD equivalent
          </p>
        </div>
        <div className="flex flex-row items-baseline gap-2 md:flex-col md:items-end md:gap-1">
          <p className="text-right text-lg font-bold md:text-3xl">
            {formatTotalVolume(tvlData[tvlData.length - 1]?.totalValue)}$
          </p>
          <p className="flex gap-1 text-right text-xs font-bold md:text-base">
            {comparePerTvlLock(10.22, 10)}/ 7 days
          </p>
        </div>
      </div>
      <ApexChart data={tvlData} />
      <hr className="w-full border-gray-200 dark:border-gray-850 md:border-t-2 mt-4 hidden md:mt-6 md:block" />
      <DataTable />
    </div>
  );
};
