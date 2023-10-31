import React, { memo, useEffect, useState } from "react";
import {
  formatStringToNumber,
  formatTotalVolume,
  formatPercentage,
} from "../utils";
import { CircularProgress } from "@mui/material";
import classNames from "classnames";
import { icons } from "../icons";

export default function DataTable(props) {
  return (
    <div className="table-wrapper">
      <table className="ranking-table">
        <thead>
          <tr className="tbrow-bg !text-[#A8AEBA]">
            <th className="min-w-[48px] md:w-[48px]">
              <div className="flex justify-center gap-2">#</div>
            </th>
            <th className="w-[160px]">
              <div className="flex justify-center gap-2">Name</div>
            </th>
            <th className="w-[105px]">
              <div className="flex justify-center gap-2">Reputation</div>
            </th>
            <th className="w-[105px]">
              <div className="flex justify-center gap-2">TVL</div>
            </th>
            {/* <th className="w-[105px]">
              <div className="flex justify-center gap-2">1D change</div>
            </th> */}
            <th className="w-[105px]">
              <div className="flex justify-center gap-2">TVL shares</div>
            </th>
            <th className="w-[105px]">
              <div className="flex justify-center gap-2">Daily TXNs</div>
            </th>
            <th className="w-[105px]">
              <div className="flex justify-center gap-2">Daily TX Vol</div>
            </th>
            {/* <th className="w-[105px]">
              <div className="flex justify-center gap-2">UAWs</div>
            </th>
            <th className="w-[105px]">
              <div className="flex justify-center gap-2">1D change</div>
            </th> */}
          </tr>
        </thead>
        <tbody>
          <TvlRankingData />
        </tbody>
      </table>
    </div>
  );
}

export const TvlRankingData = () => {
  const [data, setData] = useState([]);

  useEffect(() => {
    const getDataProject = async () => {
      try {
        const { data } = await fetch(
          `https://l2s-api.octan.network/api/tvl/projects`,
          {
            method: "GET",
            headers: {
              "Content-Type": "application/json",
            },
          }
        ).then((res) => res.json());

        setData(data);
      } catch (e) {
        console.log(e);
      }
    };
    getDataProject();
  }, []);

  return (
    <>
      {data.length > 0 ? (
        data?.map((item, index) => {
          return <TvlRankingDataItem key={index} item={item} index={index} />;
        })
      ) : (
        <tr>
          <td colSpan={10}>
            <div className="flex justify-center m-4 max-[1050px]:pr-[50px] max-[900px]:pr-[100px] max-[800px]:pr-[200px] max-[675px]:pr-[300px] max-[500px]:pr-[400px] max-[450px]:pr-[450px] max-[400px]:pr-[500px] max-[350px]:pr-[550px] max-[300px]:pr-[550px]">
              <CircularProgress />
            </div>
          </td>
        </tr>
      )}
    </>
  );
};

export const TvlRankingDataItem = memo(({ item, index }) => {
  const [hoveredRow, setHoveredRow] = useState(-1);

  const hoverStyles = (index) => {
    return index === hoveredRow ? "bg-hover" : "tbrow-bg";
  };

  const changeStyles = (num) => {
    return num > 0 ? "text-[#50E35F]" : "text-[#FA3A3A]";
  };

  return (
    <>
      <tr
        key={item.id}
        onMouseEnter={() => setHoveredRow(index)}
        onMouseLeave={() => setHoveredRow(-1)}
        className="text-black"
      >
        <td className={classNames(hoverStyles(index), "text-center")}>
          <span>{item.rank}</span>
        </td>
        <td className={classNames(hoverStyles(index), "text-center")}>
          <span>{item.name ? item.name : "-"}</span>
        </td>
        <td className={classNames(hoverStyles(index), "text-center")}>
          <span className="linear-text">
            {item.totalGrs ? formatStringToNumber(item.totalGrs, 0) : "-"}
          </span>
        </td>
        <td className={classNames(hoverStyles(index), "text-center")}>
          <div className="flex justify-center items-center gap-1">
            <span>{formatTotalVolume(item.tvl)}</span>
            <span
              className={classNames(
                "flex items-center gap-1",
                changeStyles(item.change)
              )}
            >
              {item.change > 0 ? (
                <img src={icons.triangleUp} alt="" className="h-3 w-3" />
              ) : (
                <img src={icons.triangleDown} alt="" className="h-3 w-3" />
              )}
              {item.change
                ? Math.abs(
                    formatStringToNumber(formatPercentage(item.change), 2)
                  )
                : "-"}
              %
            </span>
          </div>
        </td>
        {/* <td
          className={classNames(
            hoverStyles(index),
            changeStyles(item.change),
            "text-center"
          )}
        >
          <span>
            {item.change
              ? formatStringToNumber(formatPercentage(item.change), 2)
              : "-"}
            %
          </span>
        </td> */}
        <td className={classNames(hoverStyles(index), "text-center")}>
          <span>
            {item.marketShare
              ? formatStringToNumber(formatPercentage(item.marketShare, 2))
              : "-"}
            %
          </span>
        </td>
        <td className={classNames(hoverStyles(index), "text-center")}>
          <span>{item.totalTxn ? formatTotalVolume(item.totalTxn) : "-"}</span>
        </td>
        <td className={classNames(hoverStyles(index), "text-center")}>
          <span>
            {item.totalTxVolume ? formatTotalVolume(item.totalTxVolume) : "-"}
          </span>
        </td>
        {/* <td className={classNames(hoverStyles(index), "text-center")}>
          <span>-</span>
        </td>
        <td className={classNames(hoverStyles(index), "text-center")}>
          <span>-</span>
        </td> */}
      </tr>
    </>
  );
});
