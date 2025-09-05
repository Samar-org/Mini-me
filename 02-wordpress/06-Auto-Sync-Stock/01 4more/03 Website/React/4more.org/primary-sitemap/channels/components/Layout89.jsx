"use client";

import { Button } from "@relume_io/relume-ui";
import React from "react";
import { RxChevronRight } from "react-icons/rx";

export function Layout89() {
  return (
    <section id="relume" className="px-[5%] py-16 md:py-24 lg:py-28">
      <div className="container">
        <div className="mb-12 grid grid-cols-1 items-start justify-between gap-x-12 gap-y-5 md:mb-18 md:grid-cols-2 md:gap-x-12 md:gap-y-8 lg:mb-20 lg:gap-x-20">
          <div>
            <p className="mb-3 font-semibold md:mb-4">Bidding</p>
            <h3 className="text-5xl font-bold leading-[1.2] md:text-7xl lg:text-8xl">
              Experience the Thrill of Online Auctions
            </h3>
          </div>
          <div>
            <p className="md:text-md">
              Bid4more is your gateway to incredible deals through exciting
              online auctions. Join a community of bidders and compete for
              valuable items at prices that can be as low as 90% off retail.
              With a wide range of products available, every auction is a chance
              to snag a bargain and enjoy the thrill of the win.
            </p>
            <div className="mt-6 flex flex-wrap items-center gap-4 md:mt-8">
              <Button title="Join" variant="secondary">
                Join
              </Button>
              <Button
                title="Bid"
                variant="link"
                size="link"
                iconRight={<RxChevronRight />}
              >
                Bid
              </Button>
            </div>
          </div>
        </div>
        <img
          src="https://d22po4pjz3o32e.cloudfront.net/placeholder-image-landscape.svg"
          className="w-full object-cover"
          alt="Relume placeholder image"
        />
      </div>
    </section>
  );
}
