"use client";

import {
  Carousel,
  CarouselContent,
  CarouselItem,
  CarouselNext,
  CarouselPrevious,
  Dialog,
  DialogContent,
  DialogTrigger,
  VideoIframe,
} from "@relume_io/relume-ui";
import React, { useEffect, useState } from "react";
import { BiSolidStar } from "react-icons/bi";
import { FaCirclePlay } from "react-icons/fa6";

const useCarousel = () => {
  const [api, setApi] = useState();
  const [current, setCurrent] = useState(0);

  useEffect(() => {
    if (!api) {
      return;
    }
    setCurrent(api.selectedScrollSnap() + 1);
    api.on("select", () => {
      setCurrent(api.selectedScrollSnap() + 1);
    });
  }, [api]);

  const handleDotClick = (index) => () => {
    // No TypeScript annotation
    if (api) {
      api.scrollTo(index);
    }
  };

  const dotClassName = (index) => {
    return `mx-[3px] inline-block size-2 rounded-full ${
      current === index + 1 ? "bg-black" : "bg-neutral-light"
    }`;
  };

  return { api, setApi, current, handleDotClick, dotClassName };
};

export function Testimonial16() {
  const useCarousel = useCarousel();
  return (
    <section
      id="relume"
      className="overflow-hidden px-[5%] py-16 md:py-24 lg:py-28"
    >
      <div className="container">
        <Carousel
          setApi={useCarousel.setApi}
          opts={{ loop: true, align: "start" }}
          className="overflow-hidden"
        >
          <div className="relative pt-20 md:pb-20 md:pt-0">
            <CarouselContent className="ml-0">
              <CarouselItem className="pl-0">
                <div className="grid w-full auto-cols-fr grid-cols-1 items-center justify-center gap-12 md:grid-cols-2 md:gap-10 lg:gap-x-20">
                  <div className="order-last md:order-first">
                    <Dialog>
                      <DialogTrigger className="relative flex w-full items-center justify-center">
                        <img
                          src="https://d22po4pjz3o32e.cloudfront.net/placeholder-video-thumbnail.svg"
                          alt="Testimonial image 1"
                          className="size-full object-cover"
                        />
                        <span className="absolute inset-0 z-10 bg-black/50" />
                        <FaCirclePlay className="absolute z-20 size-16 text-white" />
                      </DialogTrigger>
                      <DialogContent>
                        <VideoIframe video="https://www.youtube.com/embed/8DKLYsikxTs?si=Ch9W0KrDWWUiCMMW" />
                      </DialogContent>
                    </Dialog>
                  </div>
                  <div className="flex flex-col items-start">
                    <div className="mb-6 flex md:mb-8">
                      <BiSolidStar className="size-6" />
                      <BiSolidStar className="size-6" />
                      <BiSolidStar className="size-6" />
                      <BiSolidStar className="size-6" />
                      <BiSolidStar className="size-6" />
                    </div>
                    <blockquote className="text-xl font-bold md:text-2xl">
                      "I never thought I could win such amazing items at such
                      low prices! Bid4more has completely changed my shopping
                      experience for the better."
                    </blockquote>
                    <div className="mt-6 flex flex-nowrap items-center gap-5 md:mt-8">
                      <div>
                        <p className="font-semibold">Alice Johnson</p>
                        <p>Marketing Manager, TechCo</p>
                      </div>
                      <div className="mx-4 w-px self-stretch bg-background-alternative sm:mx-0" />
                      <div>
                        <img
                          src="https://d22po4pjz3o32e.cloudfront.net/webflow-logo.svg"
                          alt="Webflow logo 1"
                          className="max-h-12"
                        />
                      </div>
                    </div>
                  </div>
                </div>
              </CarouselItem>
              <CarouselItem className="pl-0">
                <div className="grid w-full auto-cols-fr grid-cols-1 items-center justify-center gap-12 md:grid-cols-2 md:gap-10 lg:gap-x-20">
                  <div className="order-last md:order-first">
                    <Dialog>
                      <DialogTrigger className="relative flex w-full items-center justify-center">
                        <img
                          src="https://d22po4pjz3o32e.cloudfront.net/placeholder-video-thumbnail.svg"
                          alt="Testimonial image 1"
                          className="size-full object-cover"
                        />
                        <span className="absolute inset-0 z-10 bg-black/50" />
                        <FaCirclePlay className="absolute z-20 size-16 text-white" />
                      </DialogTrigger>
                      <DialogContent>
                        <VideoIframe video="https://www.youtube.com/embed/8DKLYsikxTs?si=Ch9W0KrDWWUiCMMW" />
                      </DialogContent>
                    </Dialog>
                  </div>
                  <div className="flex flex-col items-start">
                    <div className="mb-6 flex md:mb-8">
                      <BiSolidStar className="size-6" />
                      <BiSolidStar className="size-6" />
                      <BiSolidStar className="size-6" />
                      <BiSolidStar className="size-6" />
                      <BiSolidStar className="size-6" />
                    </div>
                    <blockquote className="text-xl font-bold md:text-2xl">
                      "I never thought I could win such amazing items at such
                      low prices! Bid4more has completely changed my shopping
                      experience for the better."
                    </blockquote>
                    <div className="mt-6 flex flex-nowrap items-center gap-5 md:mt-8">
                      <div>
                        <p className="font-semibold">Alice Johnson</p>
                        <p>Marketing Manager, TechCo</p>
                      </div>
                      <div className="mx-4 w-px self-stretch bg-background-alternative sm:mx-0" />
                      <div>
                        <img
                          src="https://d22po4pjz3o32e.cloudfront.net/webflow-logo.svg"
                          alt="Webflow logo 1"
                          className="max-h-12"
                        />
                      </div>
                    </div>
                  </div>
                </div>
              </CarouselItem>
            </CarouselContent>
            <div className="absolute top-0 flex w-full items-start justify-between md:bottom-0 md:top-auto md:items-end">
              <div className="mt-2.5 flex w-full items-start justify-start md:mb-2.5 md:mt-0">
                <button
                  onClick={useCarousel.handleDotClick(0)}
                  className={useCarousel.dotClassName(0)}
                />
                <button
                  onClick={useCarousel.handleDotClick(1)}
                  className={useCarousel.dotClassName(1)}
                />
              </div>
              <div className="flex items-end justify-end gap-2 md:gap-4">
                <CarouselPrevious className="static right-0 top-0 size-12 -translate-y-0" />
                <CarouselNext className="static right-0 top-0 size-12 -translate-y-0" />
              </div>
            </div>
          </div>
        </Carousel>
      </div>
    </section>
  );
}
